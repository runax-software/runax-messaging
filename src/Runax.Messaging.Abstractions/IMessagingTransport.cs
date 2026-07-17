namespace Runax.Messaging.Abstractions;

/// <summary>
/// Provider SPI for messaging transports. Each transport (in-memory, RabbitMQ, SQS, ...)
/// implements this to handle broker-specific publish and subscribe operations.
/// </summary>
public interface IMessagingTransport
{
    /// <summary>
    /// Publishes a serialized envelope to the specified topic.
    /// </summary>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="envelopeJson">The serialized message envelope.</param>
    /// <param name="cancellationToken">Token to cancel the publish operation.</param>
    /// <returns>A task that completes once the envelope has been sent to the broker.</returns>
    ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to topics and invokes the callback for each received message. Blocks until cancellation.
    /// </summary>
    /// <param name="topics">Topics to subscribe to.</param>
    /// <param name="onMessage">Callback invoked with (envelopeJson, topic) for each message.</param>
    /// <param name="cancellationToken">Token that stops the subscriptions.</param>
    /// <returns>A task that runs until <paramref name="cancellationToken"/> is signaled.</returns>
    Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask> onMessage,
        CancellationToken cancellationToken = default);
}
