namespace Runax.Messaging;

/// <summary>
/// Built-in strategies for a message that no registered consumer accepts. Selected with
/// <c>OnUnroutableMessage(...)</c>; for anything else, register a custom
/// <see cref="Abstractions.IUnroutableMessageHandler"/>.
/// </summary>
public enum UnroutableStrategy
{
    /// <summary>
    /// Route the message through the configured dead-letter strategy. This is the default.
    /// </summary>
    DeadLetter,

    /// <summary>
    /// Redeliver the message. Use with care: if no consumer ever handles the version this loops.
    /// </summary>
    Requeue,

    /// <summary>
    /// Acknowledge and drop the message.
    /// </summary>
    Discard,
}
