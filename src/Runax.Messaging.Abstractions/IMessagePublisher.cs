namespace Runax.Messaging.Abstractions;

/// <summary>
/// Publishes messages to a topic. The underlying transport determines
/// how topics map to broker-specific constructs.
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Publishes a message to the specified topic.
    /// </summary>
    ValueTask PublishAsync<TMessage>(string topic, TMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message to the specified topic with custom headers.
    /// </summary>
    ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default);
}
