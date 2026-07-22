using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.ServiceBus;

/// <summary>
/// Health check that reports whether the Service Bus transport can reach the namespace.
/// </summary>
internal sealed class AzureServiceBusHealthCheck(IEnumerable<IMessagingTransport> transports) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transports.OfType<AzureServiceBusTransport>().FirstOrDefault() is not { } serviceBus)
            return HealthCheckResult.Unhealthy("The registered messaging transport is not Azure Service Bus.");

        try
        {
            await serviceBus.PingAsync(cancellationToken);
            return HealthCheckResult.Healthy("Service Bus namespace is reachable.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Failed to reach Azure Service Bus.", ex);
        }
    }
}
