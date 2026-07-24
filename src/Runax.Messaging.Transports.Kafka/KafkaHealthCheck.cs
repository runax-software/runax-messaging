using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Kafka;

/// <summary>
/// Health check that reports whether the Kafka transport can reach the cluster.
/// </summary>
internal sealed class KafkaHealthCheck(IEnumerable<IMessagingTransport> transports) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transports.OfType<KafkaTransport>().FirstOrDefault() is not { } kafka)
            return HealthCheckResult.Unhealthy("The registered messaging transport is not Kafka.");

        try
        {
            return await kafka.PingAsync(cancellationToken)
                ? HealthCheckResult.Healthy("Kafka cluster is reachable.")
                : HealthCheckResult.Unhealthy("Kafka cluster returned no brokers.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Failed to reach Kafka.", ex);
        }
    }
}
