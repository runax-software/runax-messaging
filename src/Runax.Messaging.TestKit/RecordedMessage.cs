using Runax.Messaging.Abstractions;

namespace Runax.Messaging.TestKit;

/// <summary>
/// A single message the harness observed flowing through the in-memory transport, together with the
/// <see cref="Disposition"/> the dispatch pipeline returned for it (acknowledged, requeued for retry, or
/// dead-lettered).
/// </summary>
public sealed class RecordedMessage
{
    internal RecordedMessage(string topic, MessageContext context, MessageDisposition disposition)
    {
        Topic = topic;
        Context = context;
        Disposition = disposition;
    }

    /// <summary>
    /// Gets the topic the message was delivered on. Framework-managed dead-lettered messages reappear on the
    /// dead-letter topic (the original topic plus <c>.dead-letter</c>) as a separate <see cref="RecordedMessage"/>.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the decoded message context — the raw body, headers, and (when present) the contract name and
    /// version. Use <see cref="MessageContext.Deserialize{T}"/> to read the payload as a typed object.
    /// </summary>
    public MessageContext Context { get; }

    /// <summary>
    /// Gets the disposition the dispatch pipeline returned for this delivery:
    /// <see cref="MessageDisposition.Acknowledge"/> when a consumer handled it (or it was framework-dead-lettered),
    /// <see cref="MessageDisposition.Requeue"/> when it was redelivered, or <see cref="MessageDisposition.DeadLetter"/>
    /// when rejected for broker-native dead-lettering.
    /// </summary>
    public MessageDisposition Disposition { get; }

    /// <summary>
    /// Deserializes the observed body into <typeparamref name="TMessage"/> using the same serializer the
    /// message was written with.
    /// </summary>
    /// <typeparam name="TMessage">The type to deserialize the body into.</typeparam>
    /// <returns>The deserialized payload, or <see langword="null"/> if the body is JSON null.</returns>
    public TMessage? As<TMessage>() => Context.Deserialize<TMessage>();
}
