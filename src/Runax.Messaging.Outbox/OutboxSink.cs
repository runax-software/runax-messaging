namespace Runax.Messaging.Outbox;

/// <summary>
/// The outbox publish sink: swapped into a bus's publish pipeline by <c>AddOutbox</c>, it writes
/// serialized envelopes to the <see cref="IOutboxStore"/> instead of the transport, so the write
/// can share the caller's transaction. The <see cref="OutboxDispatcher"/> later delivers them.
/// </summary>
internal sealed class OutboxSink(string busName, IOutboxStore store) : IBusPublishSink
{
    public async ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken) =>
        await store.AddAsync(
            new OutboxMessage { Bus = busName, Topic = topic, Payload = envelopeJson }, cancellationToken)
            .ConfigureAwait(false);

    public async ValueTask PublishBatchAsync(
        string topic, IReadOnlyList<string> envelopeJsons, CancellationToken cancellationToken)
    {
        foreach (var envelopeJson in envelopeJsons)
            await PublishAsync(topic, envelopeJson, cancellationToken).ConfigureAwait(false);
    }
}
