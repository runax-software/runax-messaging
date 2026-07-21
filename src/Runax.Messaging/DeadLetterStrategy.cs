namespace Runax.Messaging;

/// <summary>
/// Selects how exhausted or poison messages are dead-lettered.
/// </summary>
public enum DeadLetterStrategy
{
    /// <summary>
    /// The dispatch pipeline republishes the message to a dead-letter topic
    /// (<c>{topic}{DeadLetterTopicSuffix}</c>) and acknowledges the original. Works on every transport.
    /// </summary>
    FrameworkManaged,

    /// <summary>
    /// The dispatch pipeline rejects the message so the broker's native dead-letter mechanism
    /// handles it (RabbitMQ dead-letter exchange, SQS redrive policy). Requires transport-side
    /// configuration; transports without native support discard the message.
    /// </summary>
    BrokerNative
}
