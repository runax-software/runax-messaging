namespace Runax.Messaging.Abstractions;

/// <summary>
/// Provider SPI for messaging transports. Each transport (in-memory, RabbitMQ, SQS, ...)
/// implements this to handle broker-specific publish and subscribe operations.
/// </summary>
public interface IMessagingTransport
{
    /// <summary>
    /// Gets the messaging system identifier for this transport (e.g. "rabbitmq", "sqs", "in-memory").
    /// Used as the <c>messaging.system</c> tag on telemetry, following OpenTelemetry conventions.
    /// </summary>
    string SystemName { get; }

    /// <summary>
    /// Publishes a serialized envelope to the specified topic.
    /// </summary>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="envelopeJson">The serialized message envelope.</param>
    /// <param name="cancellationToken">Token to cancel the publish operation.</param>
    /// <returns>A task that completes once the envelope has been sent to the broker.</returns>
    ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes several serialized envelopes to the same topic. The default implementation publishes
    /// them one at a time; transports override it to use a broker-native batch API.
    /// </summary>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="envelopeJsons">The serialized message envelopes.</param>
    /// <param name="cancellationToken">Token to cancel the publish operation.</param>
    /// <returns>A task that completes once every envelope has been sent to the broker.</returns>
    async ValueTask PublishBatchAsync(
        string topic,
        IReadOnlyList<string> envelopeJsons,
        CancellationToken cancellationToken = default)
    {
        foreach (var envelopeJson in envelopeJsons)
            await PublishAsync(topic, envelopeJson, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Subscribes to topics and invokes the callback for each received message. Blocks until cancellation.
    /// </summary>
    /// <param name="topics">Topics to subscribe to.</param>
    /// <param name="onMessage">
    /// Callback invoked with (envelopeJson, topic) for each message. The returned
    /// <see cref="MessageDisposition"/> tells the transport whether to acknowledge the message
    /// (remove it from the broker), requeue it for later redelivery, or dead-letter it
    /// (reject without redelivery so the broker's native dead-letter mechanism handles it).
    /// </param>
    /// <param name="cancellationToken">Token that stops the subscriptions.</param>
    /// <returns>A task that runs until <paramref name="cancellationToken"/> is signaled.</returns>
    Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default);
}
