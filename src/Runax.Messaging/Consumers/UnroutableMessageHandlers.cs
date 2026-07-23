using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Default handler: routes an unroutable message through the configured dead-letter strategy.
/// </summary>
internal sealed class DeadLetterUnroutableHandler : IUnroutableMessageHandler
{
    public ValueTask<MessageDisposition> HandleAsync(UnroutableMessage message, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(MessageDisposition.DeadLetter);
}

/// <summary>
/// Redelivers an unroutable message (beware redelivery loops if no consumer ever appears).
/// </summary>
internal sealed class RequeueUnroutableHandler : IUnroutableMessageHandler
{
    public ValueTask<MessageDisposition> HandleAsync(UnroutableMessage message, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(MessageDisposition.Requeue);
}

/// <summary>
/// Acknowledges and drops an unroutable message.
/// </summary>
internal sealed class DiscardUnroutableHandler : IUnroutableMessageHandler
{
    public ValueTask<MessageDisposition> HandleAsync(UnroutableMessage message, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(MessageDisposition.Acknowledge);
}
