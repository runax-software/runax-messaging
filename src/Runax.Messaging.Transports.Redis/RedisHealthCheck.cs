using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Redis;

/// <summary>
/// Health check that reports whether the Redis transport can reach the server.
/// </summary>
internal sealed class RedisHealthCheck(IEnumerable<IMessagingTransport> transports) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transports.OfType<RedisTransport>().FirstOrDefault() is not { } redis)
            return HealthCheckResult.Unhealthy("The registered messaging transport is not Redis.");

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
