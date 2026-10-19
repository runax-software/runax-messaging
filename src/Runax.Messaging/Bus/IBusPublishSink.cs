using Runax.Messaging.Abstractions;

namespace Runax.Messaging;

/// <summary>
/// Where a bus's publish pipeline hands off serialized messages. The default sink is the bus's
/// transport; the outbox package registers a keyed replacement (per bus) that writes envelopes to
/// the outbox store instead, from which its dispatcher later delivers them to the transport.
/// </summary>
internal interface IBusPublishSink
{
    /// <summary>Hands one serialized message, with its native headers, to the sink.</summary>
    ValueTask PublishAsync(TransportMessage message, CancellationToken cancellationToken);

    /// <summary>Hands several serialized messages for the same topic to the sink.</summary>
    ValueTask PublishBatchAsync(IReadOnlyList<TransportMessage> messages, CancellationToken cancellationToken);
}
