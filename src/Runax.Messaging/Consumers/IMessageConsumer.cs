using System.Collections.Frozen;
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

    /// <summary>
    /// The contract schema <c>$id</c> of this consumer's type (its <c>Schema</c>), normalized, or
    /// <see langword="null"/> when it declares none.
    /// </summary>
    string? ContractSchema => null;

    /// <summary>
    /// The contract schema <c>$id</c>s this consumer accepts: its type's <c>Schema</c> plus <c>Reads</c>.
    /// Empty when the type declares no schema.
    /// </summary>
    FrozenSet<string> ContractSchemas => FrozenSet<string>.Empty;

    ValueTask HandleAsync(MessageContext context, CancellationToken cancellationToken = default);
}
