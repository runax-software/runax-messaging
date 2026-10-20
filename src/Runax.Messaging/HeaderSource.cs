namespace Runax.Messaging;

/// <summary>
/// Selects which copy of a header wins when a received message carries it both as a native broker header
/// and in the envelope's reserved <c>__runax</c> metadata.
/// </summary>
public enum HeaderSource
{
    /// <summary>
    /// The native broker header wins. This is what producers outside this library and broker-level tooling
    /// (tracing instrumentation, routing rules) set.
    /// </summary>
    Broker,

    /// <summary>
    /// The envelope's copy wins.
    /// </summary>
    Envelope,
}
