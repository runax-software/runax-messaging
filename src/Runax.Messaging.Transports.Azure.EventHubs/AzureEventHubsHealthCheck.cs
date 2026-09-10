using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.EventHubs;

/// <summary>
/// Health check that reports whether one bus's Event Hubs transport can reach the namespace by
/// fetching properties for an event hub. Registered automatically per bus as <c>runax:{bus}</c>.
/// </summary>
internal sealed class AzureEventHubsHealthCheck(IMessagingTransport transport) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transport is not AzureEventHubsTransport eventHubs)
            return HealthCheckResult.Unhealthy("The bus's messaging transport is not Azure Event Hubs.");

        try
        {
            await eventHubs.PingAsync(cancellationToken);
            return HealthCheckResult.Healthy("Event Hubs namespace is reachable.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Failed to reach Azure Event Hubs.", ex);
        }
    }
}
