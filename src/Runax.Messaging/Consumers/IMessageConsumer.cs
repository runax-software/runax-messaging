using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Internal dispatch interface used by the hosting infrastructure.
/// </summary>
internal interface IMessageConsumer
{
    string Topic { get; }

    ValueTask HandleAsync(MessageContext context, CancellationToken cancellationToken = default);
}
