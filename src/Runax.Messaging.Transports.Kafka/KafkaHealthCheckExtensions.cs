using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Kafka;

/// <summary>
/// Health check registration extensions for the Kafka transport.
/// </summary>
public static class KafkaHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check that verifies the Kafka cluster is reachable.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check name. Defaults to "kafka".</param>
    /// <param name="failureStatus">The status reported on failure. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Optional tags used to filter the check.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static IHealthChecksBuilder AddKafkaTransport(
        this IHealthChecksBuilder builder,
        string name = "kafka",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
        => builder.Add(new HealthCheckRegistration(
            name,
            sp => new KafkaHealthCheck(sp.GetServices<IMessagingTransport>()),
            failureStatus,
            tags));
}
