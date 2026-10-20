using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.ServiceBus;

/// <summary>
/// Azure Service Bus implementation of <see cref="IMessagingTransport"/>. Publishing sends to a topic;
/// consuming runs a processor over a subscription. Native headers travel as application properties. The
/// <c>DeadLetter</c> disposition uses Service Bus's native dead-letter queue.
/// </summary>
internal sealed class AzureServiceBusTransport : IMessagingTransport, IDisposable
{
    private readonly AzureServiceBusConfig _options;
    private readonly ILogger<AzureServiceBusTransport> _logger;
    private readonly ServiceBusClient _client;
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new();

    public AzureServiceBusTransport(AzureServiceBusConfig options, ILogger<AzureServiceBusTransport> logger)
    {
        _options = options;
        _logger = logger;
        _client = new ServiceBusClient(options.ConnectionString);
    }

    internal const string TransportName = "servicebus";

    public string SystemName => TransportName;

    public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default) =>
        PublishAsync(new TransportMessage(topic, envelopeJson), cancellationToken);

    public async ValueTask PublishAsync(TransportMessage message, CancellationToken cancellationToken = default)
    {
        var sender = _senders.GetOrAdd(message.Topic, _client.CreateSender);
        await sender.SendMessageAsync(ToServiceBusMessage(message), cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask PublishBatchAsync(
        IReadOnlyList<TransportMessage> messages,
        CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
            await PublishAsync(message, cancellationToken).ConfigureAwait(false);
    }

    public Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default) =>
        SubscribeAsync(topics, message => onMessage(message.Body, message.Topic), cancellationToken);

    public async Task SubscribeAsync(
        string[] topics,
        Func<TransportMessage, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        var processors = new List<ServiceBusProcessor>();
        foreach (var topic in topics)
        {
            if (!_options.TopicSubscriptionMap.TryGetValue(topic, out var subscription))
            {
                _logger.LogWarning(
                    "No subscription mapped for topic '{Topic}'; it cannot be consumed. Add it to TopicSubscriptionMap.", topic);
                continue;
            }

            var processor = _client.CreateProcessor(topic, subscription, new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = _options.MaxConcurrentCalls,
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            });

            var subscriptionTopic = topic;
            processor.ProcessMessageAsync += args => OnMessageAsync(args, subscriptionTopic, onMessage);
            processor.ProcessErrorAsync += args =>
            {
                _logger.LogError(args.Exception, "Service Bus processor error on {Entity}", args.EntityPath);
                return Task.CompletedTask;
            };

            await processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
            processors.Add(processor);
            _logger.LogInformation("Subscribed to Service Bus {Topic}/{Subscription}", topic, subscription);
        }

        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Service Bus consumer shutting down");
        }
        finally
        {
            foreach (var processor in processors)
            {
                try
                {
                    await processor.StopProcessingAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error stopping a Service Bus processor.");
                }

                await processor.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async Task OnMessageAsync(
        ProcessMessageEventArgs args,
        string topic,
        Func<TransportMessage, ValueTask<MessageDisposition>> onMessage)
    {
        MessageDisposition disposition;
        try
        {
            disposition = await onMessage(new TransportMessage(
                topic, args.Message.Body.ToString(), ReadHeaders(args.Message.ApplicationProperties)))
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error dispatching Service Bus message on '{Topic}'; abandoning.", topic);
            disposition = MessageDisposition.Requeue;
        }

        switch (disposition)
        {
            case MessageDisposition.Acknowledge:
                await args.CompleteMessageAsync(args.Message, args.CancellationToken).ConfigureAwait(false);
                break;
            case MessageDisposition.DeadLetter:
                // Service Bus has a native dead-letter queue for the subscription.
                await args.DeadLetterMessageAsync(args.Message, cancellationToken: args.CancellationToken).ConfigureAwait(false);
                break;
            case MessageDisposition.Requeue:
            default:
                await args.AbandonMessageAsync(args.Message, cancellationToken: args.CancellationToken).ConfigureAwait(false);
                break;
        }
    }

    internal static ServiceBusMessage ToServiceBusMessage(TransportMessage message)
    {
        var serviceBusMessage = new ServiceBusMessage(message.Body);
        foreach (var (key, value) in message.Headers)
            serviceBusMessage.ApplicationProperties[key] = value;

        return serviceBusMessage;
    }

    internal static Dictionary<string, string>? ReadHeaders(IReadOnlyDictionary<string, object> properties)
    {
        if (properties.Count == 0)
            return null;

        var headers = new Dictionary<string, string>(properties.Count, StringComparer.Ordinal);
        foreach (var (key, value) in properties)
        {
            if (MessageHeaders.FormatValue(value) is { } text)
                headers[key] = text;
        }

        return headers;
    }

    /// <summary>
    /// Verifies reachability by fetching namespace properties through the management endpoint.
    /// </summary>
    internal async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        var admin = new ServiceBusAdministrationClient(_options.ConnectionString);
        await admin.GetNamespacePropertiesAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    public void Dispose()
    {
        foreach (var sender in _senders.Values)
            sender.DisposeAsync().AsTask().GetAwaiter().GetResult();

        _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
