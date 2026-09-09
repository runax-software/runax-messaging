using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Redis;

/// <summary>
/// Health check that reports whether one bus's Redis transport can reach the server.
/// Registered automatically per bus as <c>runax:{bus}</c>.
/// </summary>
internal sealed class RedisHealthCheck(IMessagingTransport transport) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transport is not RedisTransport redis)
            return HealthCheckResult.Unhealthy("The bus's messaging transport is not Redis.");

        try
        {
            await redis.PingAsync(cancellationToken);
            return HealthCheckResult.Healthy("Redis is reachable.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Failed to reach Redis.", ex);
        }
    }
}
