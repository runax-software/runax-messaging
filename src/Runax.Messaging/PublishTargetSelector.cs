using Runax.Messaging.Abstractions;

namespace Runax.Messaging;

/// <summary>
/// Selects the single transport a publish path (the publisher or the outbox dispatcher) sends to when
/// one or more transports are registered, honoring the <c>PublishTo</c> selection.
/// </summary>
internal static class PublishTargetSelector
{
    public static IMessagingTransport Select(
        IReadOnlyList<IMessagingTransport> transports,
        string? defaultTransportName)
    {
        if (defaultTransportName is not null)
        {
            return transports.FirstOrDefault(t => t.SystemName == defaultTransportName)
                ?? throw new InvalidOperationException(
                    $"No registered transport reports the system name '{defaultTransportName}' selected via PublishTo(...). " +
                    $"Registered transports: {Describe(transports)}.");
        }

        return transports.Count switch
        {
            0 => throw new InvalidOperationException(
                "No messaging transport is registered. Add one (e.g. AddInMemory(), AddRabbitMq(...)) before publishing."),
            1 => transports[0],
            _ => throw new InvalidOperationException(
                "Multiple messaging transports are registered. Call PublishTo(\"<system-name>\") to choose which one " +
                $"publishes are routed to. Registered transports: {Describe(transports)}."),
        };
    }

    private static string Describe(IReadOnlyList<IMessagingTransport> transports) =>
        transports.Count == 0 ? "(none)" : string.Join(", ", transports.Select(t => t.SystemName));
}
