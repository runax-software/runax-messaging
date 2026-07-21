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
    Requeue,

    /// <summary>
    /// The message must not be redelivered; reject it so the broker routes it to its
    /// native dead-letter mechanism (RabbitMQ dead-letter exchange, SQS redrive policy).
    /// Transports without a native dead-letter facility discard the message.
    /// </summary>
    DeadLetter
}
