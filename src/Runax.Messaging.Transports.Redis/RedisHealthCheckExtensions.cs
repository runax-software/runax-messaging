using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Redis;

/// <summary>
/// Health check registration extensions for the Redis transport.
/// </summary>
public static class RedisHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check that verifies the Redis server is reachable.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check name. Defaults to "redis".</param>
    /// <param name="failureStatus">The status reported on failure. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Optional tags used to filter the check.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static IHealthChecksBuilder AddRedisTransport(
        this IHealthChecksBuilder builder,
        string name = "redis",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
        => builder.Add(new HealthCheckRegistration(
            name,
            sp => new RedisHealthCheck(sp.GetServices<IMessagingTransport>()),
            failureStatus,
            tags));
}
