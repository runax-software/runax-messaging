using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sns;

/// <summary>
/// Health check that reports whether the SNS transport can reach the service.
/// </summary>
internal sealed class SnsHealthCheck(IEnumerable<IMessagingTransport> transports) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transports.OfType<SnsTransport>().FirstOrDefault() is not { } sns)
            return HealthCheckResult.Unhealthy("The registered messaging transport is not SNS.");

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
