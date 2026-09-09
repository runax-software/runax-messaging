namespace Runax.Messaging;

/// <summary>
/// Where a bus's publish pipeline hands off serialized envelopes. The default sink is the bus's
/// transport; the outbox package registers a keyed replacement (per bus) that writes envelopes to
/// the outbox store instead, from which its dispatcher later delivers them to the transport.
/// </summary>
internal interface IBusPublishSink
{
    /// <summary>Hands one serialized envelope to the sink.</summary>
    ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken);

    /// <summary>Hands several serialized envelopes for the same topic to the sink.</summary>
    ValueTask PublishBatchAsync(string topic, IReadOnlyList<string> envelopeJsons, CancellationToken cancellationToken);
}
