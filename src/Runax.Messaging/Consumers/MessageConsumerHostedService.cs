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
/// Background service that subscribes registered message consumers to their topics
/// and dispatches incoming messages, applying the configured retry and dead-letter policy.
/// </summary>
internal sealed class MessageConsumerHostedService(
    IServiceProvider serviceProvider,
    IEnumerable<ConsumerRegistration> registrations,
    IMessagingTransport transport,
    IMessageSerializer serializer,
    RetryOptions retryOptions,
    ILogger<MessageConsumerHostedService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topicConsumers = new Dictionary<string, List<IMessageConsumer>>();
        foreach (var registration in registrations)
        {
            var consumer = (IMessageConsumer)serviceProvider.GetRequiredService(registration.ConsumerType);

            if (!topicConsumers.TryGetValue(consumer.Topic, out var list))
            {
                list = [];
                topicConsumers[consumer.Topic] = list;
            }

            list.Add(consumer);
        }

        var allTopics = topicConsumers.Keys.ToArray();
        if (allTopics.Length == 0)
        {
            logger.LogInformation("No topics to subscribe to. No consumers registered any topics.");
            return;
        }

        logger.LogInformation("Subscribing to {TopicCount} topic(s): {Topics}", allTopics.Length,
            string.Join(", ", allTopics));

        await transport.SubscribeAsync(
            allTopics,
            (envelopeJson, topic) => DispatchAsync(envelopeJson, topic, topicConsumers, stoppingToken),
            stoppingToken);
    }

    private async ValueTask<MessageDisposition> DispatchAsync(
        string envelopeJson,
        string topic,
        Dictionary<string, List<IMessageConsumer>> topicConsumers,
        CancellationToken cancellationToken)
    {
        if (!topicConsumers.TryGetValue(topic, out var consumers))
            return MessageDisposition.Acknowledge;

        var startTimestamp = Stopwatch.GetTimestamp();
        var tags = MessagingDiagnostics.Tags(transport.SystemName, topic);

        MessageContext context;
        try
        {
            context = serializer.Deserialize(envelopeJson, topic);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Malformed envelope on topic '{Topic}'. Dead-lettering.", topic);
            using var malformedActivity = StartProcessActivity(topic, headers: null);
            malformedActivity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            try
            {
                return await DeadLetterAsync(envelopeJson, topic, ex, attempts: 0, cancellationToken);
            }
            finally
            {
                MessagingDiagnostics.ProcessingDuration.Record(
                    Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds, tags);
            }
        }

        using var activity = StartProcessActivity(topic, context.Headers);
        try
        {
            // Requeue wins outright (redeliver the whole message); otherwise a single dead-letter
            // verdict escalates the message away from a plain acknowledge.
            var result = MessageDisposition.Acknowledge;
            foreach (var consumer in consumers)
            {
                var disposition = await DispatchToConsumerAsync(consumer, context, envelopeJson, topic, cancellationToken);
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
            activity.SetTag("messaging.system", transport.SystemName);
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation", "process");
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
        MessageContext context,
        string envelopeJson,
        string topic,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await consumer.HandleAsync(context, cancellationToken);
                MessagingDiagnostics.Consumed.Add(1, MessagingDiagnostics.Tags(transport.SystemName, topic));
                return MessageDisposition.Acknowledge;
            }
            catch (PoisonMessageException ex)
            {
                logger.LogWarning(ex,
                    "Consumer {Consumer} rejected message on '{Topic}' as poison. Dead-lettering.",
                    consumer.GetType().Name, topic);
                return await DeadLetterAsync(envelopeJson, topic, ex, attempt, cancellationToken);
            }
            catch (Exception ex) when (attempt < retryOptions.MaxAttempts && !cancellationToken.IsCancellationRequested)
            {
                var delay = ComputeBackoff(attempt);
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
                return await DeadLetterAsync(envelopeJson, topic, ex, attempt, cancellationToken);
            }
        }
    }

    private TimeSpan ComputeBackoff(int attempt)
    {
        var factor = Math.Pow(retryOptions.BackoffFactor, attempt - 1);
        var ticks = Math.Min(retryOptions.InitialDelay.Ticks * factor, retryOptions.MaxDelay.Ticks);
        return TimeSpan.FromTicks((long)ticks);
    }

    private async ValueTask<MessageDisposition> DeadLetterAsync(
        string envelopeJson,
        string topic,
        Exception exception,
        int attempts,
        CancellationToken cancellationToken)
    {
        MessagingDiagnostics.Failed.Add(1, MessagingDiagnostics.Tags(transport.SystemName, topic));

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
            var enriched = serializer.EnrichHeaders(envelopeJson, new Dictionary<string, string>
            {
                ["x-runax-dlq-reason"] = exception.Message,
                ["x-runax-dlq-exception"] = exception.GetType().FullName ?? exception.GetType().Name,
                ["x-runax-dlq-original-topic"] = topic,
                ["x-runax-dlq-attempts"] = attempts.ToString(CultureInfo.InvariantCulture),
                ["x-runax-dlq-timestamp"] = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture),
            });

            await transport.PublishAsync(deadLetterTopic, enriched, cancellationToken);
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
