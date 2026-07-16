using Runax.Messaging.Abstractions;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Bridges <see cref="IMessagePublisher"/> to the underlying <see cref="IMessagingTransport"/>,
/// handling serialization of the message into an envelope.
/// </summary>
internal sealed class MessagePublisherAdapter(IMessagingTransport transport, IMessageSerializer serializer)
    : IMessagePublisher
{
    /// <inheritdoc />
    public ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        CancellationToken cancellationToken = default) =>
        transport.PublishAsync(topic, serializer.Serialize(message, null), cancellationToken);

    /// <inheritdoc />
    public ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default) =>
        transport.PublishAsync(topic, serializer.Serialize(message, headers), cancellationToken);
}
