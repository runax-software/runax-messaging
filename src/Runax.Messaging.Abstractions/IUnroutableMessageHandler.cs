namespace Runax.Messaging.Abstractions;

/// <summary>
/// Decides what happens to a message that no registered consumer accepts (see <see cref="UnroutableMessage"/>).
/// Implement this to plug in custom behavior — for example forwarding to a quarantine topic or raising an
/// alert — then return the disposition the transport should apply to the original message.
/// </summary>
public interface IUnroutableMessageHandler
{
    /// <summary>
    /// Handles an unroutable message and returns the disposition to apply:
    /// <see cref="MessageDisposition.DeadLetter"/> routes it through the configured dead-letter strategy,
    /// <see cref="MessageDisposition.Requeue"/> redelivers it (beware redelivery loops if no consumer ever
    /// appears), and <see cref="MessageDisposition.Acknowledge"/> discards it.
    /// </summary>
    /// <param name="message">The message no consumer accepted.</param>
    /// <param name="cancellationToken">Token that is signaled when the host is shutting down.</param>
    /// <returns>The disposition the transport should apply.</returns>
    ValueTask<MessageDisposition> HandleAsync(UnroutableMessage message, CancellationToken cancellationToken = default);
}
