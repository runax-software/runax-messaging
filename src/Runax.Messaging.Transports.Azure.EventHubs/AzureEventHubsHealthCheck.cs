using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.EventHubs;

/// <summary>
/// Health check that reports whether the Event Hubs transport can reach the namespace by fetching
/// properties for a probe event hub.
/// </summary>
internal sealed class AzureEventHubsHealthCheck(IEnumerable<IMessagingTransport> transports, string eventHub) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transports.OfType<AzureEventHubsTransport>().FirstOrDefault() is not { } eventHubs)
            return HealthCheckResult.Unhealthy("The registered messaging transport is not Azure Event Hubs.");

        try
        {
            await eventHubs.PingAsync(eventHub, cancellationToken);
            return HealthCheckResult.Healthy("Event Hubs namespace is reachable.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Failed to reach Azure Event Hubs.", ex);
        }
    }
}
