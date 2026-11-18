using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// A consumer registered from a delegate on a topic chosen at runtime. It declares no contract, so
/// it receives every message on its topic, undeserialized; the bus's retry and dead-letter policy
/// apply as for any other consumer.
/// </summary>
/// <param name="topic">The topic to consume.</param>
/// <param name="handler">Handles each message.</param>
internal sealed class DelegateConsumer(
    string topic,
    Func<MessageContext, CancellationToken, ValueTask> handler) : IMessageConsumer
{
    public string Topic => topic;

    public int? ContractVersion => null;

    public ValueTask HandleAsync(MessageContext context, CancellationToken cancellationToken = default) =>
        handler(context, cancellationToken);
}
