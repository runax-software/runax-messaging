using System.Globalization;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Records why a message was unroutable when it is dead-lettered — no registered consumer accepted its
/// contract version. Used as the dead-letter reason so it surfaces in the <c>x-runax-dlq-*</c> headers.
/// </summary>
internal sealed class UnroutableMessageException(string topic, int? contractVersion)
    : Exception($"No registered consumer accepts contract version " +
                $"{(contractVersion?.ToString(CultureInfo.InvariantCulture) ?? "(unversioned)")} on topic '{topic}'.")
{
    public int? ContractVersion { get; } = contractVersion;
}
