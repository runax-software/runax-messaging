using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Outbox;

/// <summary>
/// The outbox publish sink: swapped into a bus's publish pipeline by <c>AddOutbox</c>, it writes
/// serialized envelopes to the <see cref="IOutboxStore"/> instead of the transport, so the write
/// can share the caller's transaction. The <see cref="OutboxDispatcher"/> later delivers them.
/// </summary>
internal sealed class OutboxSink(string busName, IOutboxStore store) : IBusPublishSink
{
    // Only the envelope is stored: it carries every header too, so the dispatcher rebuilds the native
    // headers from it and the store schema stays unchanged.
    public async ValueTask PublishAsync(TransportMessage message, CancellationToken cancellationToken) =>
        await store.AddAsync(
            new OutboxMessage { Bus = busName, Topic = message.Topic, Payload = message.Body }, cancellationToken)
            .ConfigureAwait(false);

    public async ValueTask PublishBatchAsync(IReadOnlyList<TransportMessage> messages, CancellationToken cancellationToken)
    {
        foreach (var message in messages)
            await PublishAsync(message, cancellationToken).ConfigureAwait(false);
    }
}
