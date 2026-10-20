using System.Globalization;
using System.Text;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Records why a message was unroutable when it is dead-lettered: no registered consumer accepted its
/// contract identity, or, for a message without one, its body. Used as the dead-letter reason so it
/// surfaces in the <c>x-runax-dlq-*</c> headers.
/// </summary>
internal sealed class UnroutableMessageException(
    string topic,
    int? contractVersion,
    string? contractSchema = null,
    UnroutableReason reason = UnroutableReason.NoMatchingContract,
    string? detail = null)
    : Exception(FormatMessage(topic, contractVersion, contractSchema, reason, detail))
{
    // Keeps the dead-letter reason header within every broker's value limits; the log has the full detail.
    private const int MaxDetailBytes = 512;

    public int? ContractVersion { get; } = contractVersion;

    public string? ContractSchema { get; } = contractSchema;

    public UnroutableReason Reason { get; } = reason;

    private static string FormatMessage(
        string topic,
        int? contractVersion,
        string? contractSchema,
        UnroutableReason reason,
        string? detail) => reason switch
        {
            UnroutableReason.NoMatchingSchema =>
                $"No consumer's schema accepts this unidentified message on topic '{topic}': {Truncate(detail)}",
            UnroutableReason.AmbiguousSchema =>
                $"This unidentified message on topic '{topic}' matches several contract schemas that reach " +
                $"different consumers: {Truncate(detail)}",
            _ when contractSchema is not null =>
                $"No registered consumer accepts contract schema '{contractSchema}' on topic '{topic}'.",
            _ =>
                "No registered consumer accepts contract version " +
                $"{contractVersion?.ToString(CultureInfo.InvariantCulture) ?? "(unversioned)"} on topic '{topic}'.",
        };

    private static string Truncate(string? detail)
    {
        if (string.IsNullOrEmpty(detail))
            return "(no detail)";

        if (Encoding.UTF8.GetByteCount(detail) <= MaxDetailBytes)
            return detail;

        var length = Math.Min(detail.Length, MaxDetailBytes);
        while (length > 0 && Encoding.UTF8.GetByteCount(detail.AsSpan(0, length)) > MaxDetailBytes - 3)
            length--;

        // Don't split a surrogate pair.
        if (length > 0 && char.IsHighSurrogate(detail[length - 1]))
            length--;

        return string.Concat(detail.AsSpan(0, length), "…");
    }
}
