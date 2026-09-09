using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.RabbitMq;

/// <summary>
/// Health check that reports whether one bus's RabbitMQ transport can reach the broker.
/// Registered automatically per bus as <c>runax:{bus}</c>.
/// </summary>
internal sealed class RabbitMqHealthCheck(IMessagingTransport transport) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transport is not RabbitMqTransport rabbitMq)
            return HealthCheckResult.Unhealthy("The bus's messaging transport is not RabbitMQ.");

        try
        {
            return await rabbitMq.PingAsync(cancellationToken)
                ? HealthCheckResult.Healthy("RabbitMQ connection is open.")
                : HealthCheckResult.Unhealthy("RabbitMQ connection is not open.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Failed to reach RabbitMQ.", ex);
        }
    }
}
