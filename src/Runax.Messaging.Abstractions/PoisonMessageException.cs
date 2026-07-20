namespace Runax.Messaging.Abstractions;

/// <summary>
/// Thrown by a consumer to signal that a message can never be processed successfully.
/// The dispatch pipeline skips any remaining retry attempts and dead-letters the message immediately.
/// </summary>
public sealed class PoisonMessageException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PoisonMessageException"/> class.
    /// </summary>
    public PoisonMessageException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="PoisonMessageException"/> class with a reason.
    /// </summary>
    /// <param name="message">Describes why the message can never be processed.</param>
    public PoisonMessageException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="PoisonMessageException"/> class with a reason and underlying cause.
    /// </summary>
    /// <param name="message">Describes why the message can never be processed.</param>
    /// <param name="innerException">The exception that caused the message to be treated as poison.</param>
    public PoisonMessageException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
