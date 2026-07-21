using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.RabbitMq;

/// <summary>
/// Health check registration extensions for the RabbitMQ transport.
/// </summary>
public static class RabbitMqHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check that verifies the RabbitMQ broker is reachable.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check name. Defaults to "rabbitmq".</param>
    /// <param name="failureStatus">The status reported on failure. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Optional tags used to filter the check.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static IHealthChecksBuilder AddRabbitMqTransport(
        this IHealthChecksBuilder builder,
        string name = "rabbitmq",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
        => builder.Add(new HealthCheckRegistration(
            name,
            sp => new RabbitMqHealthCheck(sp.GetRequiredService<IMessagingTransport>()),
            failureStatus,
            tags));
}
