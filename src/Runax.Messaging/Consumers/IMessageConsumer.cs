using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Internal dispatch interface used by the hosting infrastructure.
/// </summary>
internal interface IMessageConsumer
{
    string Topic { get; }

    /// <summary>
    /// The contract version this consumer handles, or <see langword="null"/> when it is unversioned
    /// (accepts every message on its topic).
    /// </summary>
    int? ContractVersion { get; }

    ValueTask HandleAsync(MessageContext context, CancellationToken cancellationToken = default);
}
