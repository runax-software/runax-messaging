namespace Runax.Messaging.Abstractions;

/// <summary>
/// The action a transport takes on a broker message once the dispatch pipeline
/// has finished processing it.
/// </summary>
public enum MessageDisposition
{
    /// <summary>
    /// The message was handled successfully (or dead-lettered); remove it from the broker.
    /// </summary>
    Acknowledge,

    /// <summary>
    /// The message could not be safely handled; return it to the broker for redelivery.
    /// </summary>
    Requeue
}
