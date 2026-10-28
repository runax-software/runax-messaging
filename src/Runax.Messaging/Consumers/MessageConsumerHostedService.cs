using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Diagnostics;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Background service that subscribes one bus's registered consumers to their topics and
/// dispatches incoming messages, applying the bus's retry and dead-letter policy. A message's native
/// broker headers are merged with its envelope's per the bus's <see cref="HeaderSource"/>. Each consuming
/// bus runs its own instance, so buses subscribe, run, and shut down independently.
/// </summary>
internal sealed class MessageConsumerHostedService(
    string busName,
    BusHeaderSettings headerSettings,
    BusContractSettings contractSettings,
    IServiceProvider serviceProvider,
    IEnumerable<ConsumerRegistration> registrations,
    IMessageSerializerProvider serializerProvider,
    IUnroutableMessageHandlerProvider unroutableHandlerProvider,
    IRetryOptionsProvider retryOptionsProvider,
    ILogger<MessageConsumerHostedService> logger)
    : BackgroundService
{
    private IMessagingTransport? _transport;

    private IMessagingTransport Transport =>
        _transport ??= serviceProvider.GetRequiredKeyedService<IMessagingTransport>(busName);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // topic -> consumers subscribed on this bus.
        var topicConsumers = new Dictionary<string, List<IMessageConsumer>>(StringComparer.Ordinal);

        foreach (var registration in registrations)
        {
            if (registration.Bus != busName)
                continue;

            var consumer = (IMessageConsumer)serviceProvider.GetRequiredService(registration.ConsumerType);
            if (!topicConsumers.TryGetValue(consumer.Topic, out var list))
            {
                list = [];
                topicConsumers[consumer.Topic] = list;
            }

            list.Add(consumer);
        }

        if (topicConsumers.Count == 0)
        {
            logger.LogInformation("Bus '{Bus}': no topics to subscribe to. No consumers registered any topics.", busName);
            return;
        }

        var topics = topicConsumers.Keys.ToArray();

        logger.LogInformation(
            "Bus '{Bus}': subscribing to {TopicCount} topic(s) on '{System}': {Topics}",
            busName, topics.Length, Transport.SystemName, string.Join(", ", topics));

        var router = new ContractRouter(serviceProvider.GetService<IContractSchemaMatcher>(), contractSettings);

        await Transport.SubscribeAsync(
            topics,
            message => DispatchAsync(message, topicConsumers, router, stoppingToken),
            stoppingToken);
    }

    private async ValueTask<MessageDisposition> DispatchAsync(
        TransportMessage message,
        Dictionary<string, List<IMessageConsumer>> topicConsumers,
        ContractRouter router,
        CancellationToken cancellationToken)
    {
        var topic = message.Topic;
        if (!topicConsumers.TryGetValue(topic, out var consumers))
            return MessageDisposition.Acknowledge;

        var startTimestamp = Stopwatch.GetTimestamp();
        var tags = MessagingDiagnostics.Tags(Transport.SystemName, topic, busName);
        var serializer = serializerProvider.For(busName, topic);

        MessageContext context;
        try
        {
            context = NativeHeaders.Merge(
                serializer.Deserialize(message.Body, topic), message.Headers, headerSettings.Precedence);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Malformed envelope on topic '{Topic}' (bus '{Bus}'). Dead-lettering.", topic, busName);
            using var malformedActivity = StartProcessActivity(topic, message.Headers);
            malformedActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            try
            {
                return await DeadLetterAsync(message, context: null, ex, attempts: 0, cancellationToken);
            }
            finally
            {
                MessagingDiagnostics.ProcessingDuration.Record(
                    Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds, tags);
            }
        }

        // Contract-declaring consumers accept only the identities they declare (or, for an unidentified
        // message, a body that validates against their schemas); an unversioned consumer accepts every message
        // on the topic. If none match, the message is unroutable and the configured strategy decides.
        var route = router.Route(consumers, context);
        ReportRouting(route, context, topic, tags);

        var matched = route.Matched;
        if (route.Inferred.Count > 0)
            context = WithInferred(context, route.Inferred);

        if (matched.Count == 0)
        {
            try
            {
                return await HandleUnroutableAsync(message, context, route, cancellationToken);
            }
            finally
            {
                MessagingDiagnostics.ProcessingDuration.Record(
                    Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds, tags);
            }
        }

        using var activity = StartProcessActivity(topic, context.Headers);
        if (context.ContractSchema is { } contractSchema)
            activity?.SetTag("messaging.runax.contract.schema", contractSchema);
        if (route.Inferred.Count > 0)
            activity?.SetTag("messaging.runax.contract.inferred", string.Join(",", route.Inferred));
        try
        {
            // Requeue wins outright (redeliver the whole message); otherwise a single dead-letter
            // verdict escalates the message away from a plain acknowledge.
            var result = MessageDisposition.Acknowledge;
            foreach (var consumer in matched)
            {
                var disposition = await DispatchToConsumerAsync(consumer, message, context, cancellationToken);
                if (disposition == MessageDisposition.Requeue)
                    return MessageDisposition.Requeue;
                if (disposition == MessageDisposition.DeadLetter)
                    result = MessageDisposition.DeadLetter;
            }

            if (result == MessageDisposition.DeadLetter)
                activity?.SetStatus(ActivityStatusCode.Error, "Message dead-lettered.");

            return result;
        }
        finally
        {
            MessagingDiagnostics.ProcessingDuration.Record(
                Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds, tags);
        }
    }

    private void ReportRouting(ContractRoute route, MessageContext context, string topic, TagList tags)
    {
        if (route.Ambiguous)
        {
            MessagingDiagnostics.ContractAmbiguous.Add(1, tags);
            logger.LogWarning(
                "Unidentified message on topic '{Topic}' (bus '{Bus}') matches contract schemas that reach different " +
                "consumers ({Schemas}); no schema consumer receives it.",
                topic, busName, route.Detail);
        }

        foreach (var consumer in route.Mismatched)
        {
            MessagingDiagnostics.ContractSchemaMismatch.Add(1, tags);
            logger.LogWarning(
                "Delivering a message with contract schema '{Schema}' to {Consumer} on topic '{Topic}' (bus '{Bus}') " +
                "by its version {Version}, although the consumer declares other schemas.",
                context.ContractSchema, consumer.GetType().Name, topic, busName, context.ContractVersion);
        }
    }

    private static MessageContext WithInferred(MessageContext context, IReadOnlyList<string> inferred) => new()
    {
        Topic = context.Topic,
        Body = context.Body,
        Headers = context.Headers,
        ContractName = context.ContractName,
        ContractVersion = context.ContractVersion,
        ContractSchema = context.ContractSchema,
        InferredContractSchemas = inferred,
        Serializer = context.Serializer,
    };

    private async ValueTask<MessageDisposition> HandleUnroutableAsync(
        TransportMessage message,
        MessageContext context,
        ContractRoute route,
        CancellationToken cancellationToken)
    {
        if (route.Reason == UnroutableReason.NoMatchingContract)
        {
            logger.LogWarning(
                "No consumer accepts contract {Identity} on topic '{Topic}' (bus '{Bus}').",
                context.ContractSchema is { } schema ? $"schema '{schema}'" : $"version {context.ContractVersion}",
                context.Topic, busName);
        }
        else
        {
            logger.LogWarning(
                "Unidentified message on topic '{Topic}' (bus '{Bus}') is unroutable ({Reason}): {Detail}",
                context.Topic, busName, route.Reason, route.Detail);
        }

        var unroutable = new UnroutableMessage
        {
            Topic = context.Topic,
            ContractName = context.ContractName,
            ContractVersion = context.ContractVersion,
            ContractSchema = context.ContractSchema,
            Reason = route.Reason,
            Body = context.Body,
            Headers = context.Headers,
            TransportSystemName = Transport.SystemName,
        };

        var handler = unroutableHandlerProvider.For(busName);
        var disposition = await handler.HandleAsync(unroutable, cancellationToken);

        if (disposition == MessageDisposition.DeadLetter)
        {
            var reason = new UnroutableMessageException(
                context.Topic, context.ContractVersion, context.ContractSchema, route.Reason, route.Detail);
            return await DeadLetterAsync(message, context, reason, attempts: 0, cancellationToken);
        }

        return disposition;
    }

    private Activity? StartProcessActivity(string topic, IReadOnlyDictionary<string, string>? headers)
    {
        string? traceParent = null;
        string? traceState = null;

        if (headers is not null)
            DistributedContextPropagator.Current.ExtractTraceIdAndState(headers, HeaderGetter, out traceParent, out traceState);

        var activity = MessagingDiagnostics.ActivitySource.StartActivity(
            $"{topic} process", ActivityKind.Consumer, traceParent);

        if (activity is not null)
        {
            activity.TraceStateString = traceState;
            activity.SetTag("messaging.system", Transport.SystemName);
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation", "process");
            activity.SetTag("messaging.runax.bus", busName);
        }

        return activity;
    }

    private static void HeaderGetter(
        object? carrier,
        string fieldName,
        out string? fieldValue,
        out IEnumerable<string>? fieldValues)
    {
        fieldValues = null;
        fieldValue = carrier is IReadOnlyDictionary<string, string> headers && headers.TryGetValue(fieldName, out var value)
            ? value
            : null;
    }

    private async ValueTask<MessageDisposition> DispatchToConsumerAsync(
        IMessageConsumer consumer,
        TransportMessage message,
        MessageContext context,
        CancellationToken cancellationToken)
    {
        var topic = message.Topic;
        var retryOptions = retryOptionsProvider.For(busName, topic);

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await consumer.HandleAsync(context, cancellationToken);
                MessagingDiagnostics.Consumed.Add(1, MessagingDiagnostics.Tags(Transport.SystemName, topic, busName));
                return MessageDisposition.Acknowledge;
            }
            catch (PoisonMessageException ex)
            {
                logger.LogWarning(ex,
                    "Consumer {Consumer} rejected message on '{Topic}' as poison. Dead-lettering.",
                    consumer.GetType().Name, topic);
                return await DeadLetterAsync(message, context, ex, attempt, cancellationToken);
            }
            catch (Exception ex) when (attempt < retryOptions.MaxAttempts && !cancellationToken.IsCancellationRequested)
            {
                var delay = ComputeBackoff(retryOptions, attempt);
                logger.LogWarning(ex,
                    "Consumer {Consumer} failed on '{Topic}' (attempt {Attempt}/{MaxAttempts}). Retrying in {Delay}.",
                    consumer.GetType().Name, topic, attempt, retryOptions.MaxAttempts, delay);

                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return MessageDisposition.Requeue;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Consumer {Consumer} failed on '{Topic}' after {Attempt} attempt(s). Dead-lettering.",
                    consumer.GetType().Name, topic, attempt);
                return await DeadLetterAsync(message, context, ex, attempt, cancellationToken);
            }
        }
    }

    private static TimeSpan ComputeBackoff(RetryOptions retryOptions, int attempt)
    {
        var factor = Math.Pow(retryOptions.BackoffFactor, attempt - 1);
        var ticks = Math.Min(retryOptions.InitialDelay.Ticks * factor, retryOptions.MaxDelay.Ticks);
        return TimeSpan.FromTicks((long)ticks);
    }

    // Dead-letter publishing is part of the consume pipeline and is therefore allowed even on a
    // ConsumeOnly bus (BusMode governs the application publishing surface only). Buses whose broker
    // credentials cannot write should use DeadLetterStrategy.BrokerNative or disable dead-lettering.
    // The dead-lettered copy keeps the message's headers natively too: the merged view when the envelope
    // could be decoded, else the broker's headers as received.
    private async ValueTask<MessageDisposition> DeadLetterAsync(
        TransportMessage message,
        MessageContext? context,
        Exception exception,
        int attempts,
        CancellationToken cancellationToken)
    {
        var topic = message.Topic;
        var retryOptions = retryOptionsProvider.For(busName, topic);

        MessagingDiagnostics.Failed.Add(1, MessagingDiagnostics.Tags(Transport.SystemName, topic, busName));

        if (!retryOptions.EnableDeadLettering)
        {
            logger.LogWarning("Dead-lettering disabled; dropping message from '{Topic}'.", topic);
            return MessageDisposition.Acknowledge;
        }

        if (retryOptions.Strategy == DeadLetterStrategy.BrokerNative)
        {
            logger.LogInformation(
                "Rejecting message from '{Topic}' for broker-native dead-lettering after {Attempts} attempt(s).",
                topic, attempts);
            return MessageDisposition.DeadLetter;
        }

        var deadLetterTopic = topic + retryOptions.DeadLetterTopicSuffix;

        try
        {
            var deadLetterHeaders = new Dictionary<string, string>
            {
                ["x-runax-dlq-reason"] = exception.Message,
                ["x-runax-dlq-exception"] = exception.GetType().FullName ?? exception.GetType().Name,
                ["x-runax-dlq-original-topic"] = topic,
                ["x-runax-dlq-attempts"] = attempts.ToString(CultureInfo.InvariantCulture),
                ["x-runax-dlq-timestamp"] = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture),
            };

            var enriched = serializerProvider.For(busName, topic).EnrichHeaders(message.Body, deadLetterHeaders);
            var nativeHeaders = headerSettings.WriteNativeHeaders
                ? NativeHeaders.Build(
                    context?.ContractName,
                    context?.ContractVersion,
                    context?.ContractSchema,
                    (context?.Headers ?? message.Headers).Concat(deadLetterHeaders))
                : null;

            await Transport.PublishAsync(new TransportMessage(deadLetterTopic, enriched, nativeHeaders), cancellationToken);
            logger.LogInformation("Dead-lettered message from '{Topic}' to '{DeadLetterTopic}'.", topic, deadLetterTopic);
            return MessageDisposition.Acknowledge;
        }
        catch (Exception ex)
        {
            logger.LogError(ex,
                "Failed to dead-letter message from '{Topic}' to '{DeadLetterTopic}'. Requeueing.",
                topic, deadLetterTopic);
            return MessageDisposition.Requeue;
        }
    }
}
