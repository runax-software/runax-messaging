using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.ServiceBus;

/// <summary>
/// Health check that reports whether one bus's Service Bus transport can reach the namespace.
/// Registered automatically per bus as <c>runax:{bus}</c>.
/// </summary>
internal sealed class AzureServiceBusHealthCheck(IMessagingTransport transport) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transport is not AzureServiceBusTransport serviceBus)
            return HealthCheckResult.Unhealthy("The bus's messaging transport is not Azure Service Bus.");

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
