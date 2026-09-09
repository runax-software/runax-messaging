using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Kafka;

/// <summary>
/// Health check that reports whether one bus's Kafka transport can reach the cluster.
/// Registered automatically per bus as <c>runax:{bus}</c>.
/// </summary>
internal sealed class KafkaHealthCheck(IMessagingTransport transport) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transport is not KafkaTransport kafka)
            return HealthCheckResult.Unhealthy("The bus's messaging transport is not Kafka.");

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
