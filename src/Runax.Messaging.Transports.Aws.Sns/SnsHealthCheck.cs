using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sns;

/// <summary>
/// Health check that reports whether one bus's SNS transport can reach the service.
/// Registered automatically per bus as <c>runax:{bus}</c>.
/// </summary>
internal sealed class SnsHealthCheck(IMessagingTransport transport) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transport is not SnsTransport sns)
            return HealthCheckResult.Unhealthy("The bus's messaging transport is not SNS.");

        try
        {
            await sns.PingAsync(cancellationToken);
            return HealthCheckResult.Healthy("SNS endpoint is reachable.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Failed to reach SNS.", ex);
        }
    }
}
