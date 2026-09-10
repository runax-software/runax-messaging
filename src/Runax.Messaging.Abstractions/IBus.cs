namespace Runax.Messaging.Abstractions;

/// <summary>
/// A configured bus — the single application handle to messaging. A bus wraps exactly one
/// transport, so publishing on the bus always targets that broker. Inject the interface
/// unkeyed to get the default bus, or keyed by bus name
/// (e.g. <c>[FromKeyedServices("audit")] IBus audit</c>) for a named bus.
/// </summary>
public interface IBus
{
    /// <summary>Gets the bus name this instance was registered under.</summary>
    string Name { get; }

    /// <summary>Gets the bus's declared <see cref="BusMode"/>.</summary>
    BusMode Mode { get; }

    /// <summary>
    /// Publishes a message to the specified topic on this bus's transport.
    /// </summary>
    /// <typeparam name="TMessage">The message payload type.</typeparam>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="message">The message payload.</param>
    /// <param name="cancellationToken">Token to cancel the publish operation.</param>
    /// <returns>A task that completes once the message has been handed to the transport.</returns>
    /// <exception cref="InvalidOperationException">The bus is <see cref="BusMode.ConsumeOnly"/>.</exception>
    ValueTask PublishAsync<TMessage>(string topic, TMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes a message to the specified topic on this bus's transport, with custom headers.
    /// </summary>
    /// <typeparam name="TMessage">The message payload type.</typeparam>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="message">The message payload.</param>
    /// <param name="headers">Transport-level headers to attach to the message.</param>
    /// <param name="cancellationToken">Token to cancel the publish operation.</param>
    /// <returns>A task that completes once the message has been handed to the transport.</returns>
    /// <exception cref="InvalidOperationException">The bus is <see cref="BusMode.ConsumeOnly"/>.</exception>
    ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Publishes several messages to the same topic, using the transport's batch API where available.
    /// </summary>
    /// <typeparam name="TMessage">The message payload type.</typeparam>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="messages">The message payloads.</param>
    /// <param name="cancellationToken">Token to cancel the publish operation.</param>
    /// <returns>A task that completes once the messages have been handed to the transport.</returns>
    /// <exception cref="InvalidOperationException">The bus is <see cref="BusMode.ConsumeOnly"/>.</exception>
    ValueTask PublishBatchAsync<TMessage>(
        string topic,
        IReadOnlyList<TMessage> messages,
        CancellationToken cancellationToken = default);
}
