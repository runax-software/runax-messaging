using System.Text.Json;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;

namespace Runax.Messaging;

/// <summary>
/// Base class for message consumers that handle a single message type on a single topic.
/// The framework deserializes the message body to <typeparamref name="TMessage"/>
/// before invoking <see cref="HandleAsync(TMessage, CancellationToken)"/>.
/// </summary>
public abstract class MessageConsumer<TMessage> : IMessageConsumer
{
    /// <summary>
    /// Gets the topic this consumer subscribes to.
    /// </summary>
    public abstract string Topic { get; }

    /// <summary>
    /// Handles a deserialized message.
    /// </summary>
    protected abstract ValueTask HandleAsync(TMessage message, CancellationToken cancellationToken = default);

    string IMessageConsumer.Topic => Topic;

    ValueTask IMessageConsumer.HandleAsync(MessageContext context, CancellationToken cancellationToken)
    {
        var message = JsonSerializer.Deserialize<TMessage>(context.Body)
                      ?? throw new InvalidOperationException(
                          $"Failed to deserialize message body to {typeof(TMessage).Name} on topic '{context.Topic}'.");

        return HandleAsync(message, cancellationToken);
    }
}
