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
    /// <typeparam name="TMessage">The message payload type.</typeparam>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="message">The message payload.</param>
    /// <param name="cancellationToken">Token to cancel the publish operation.</param>
    /// <returns>A task that completes once the message has been handed to the transport.</returns>
    ValueTask PublishAsync<TMessage>(string topic, TMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message to the specified topic with custom headers.
    /// </summary>
    /// <typeparam name="TMessage">The message payload type.</typeparam>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="message">The message payload.</param>
    /// <param name="headers">Transport-level headers to attach to the message.</param>
    /// <param name="cancellationToken">Token to cancel the publish operation.</param>
    /// <returns>A task that completes once the message has been handed to the transport.</returns>
    ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default);
}
