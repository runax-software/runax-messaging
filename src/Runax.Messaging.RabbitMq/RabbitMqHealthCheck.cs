using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.RabbitMq;

/// <summary>
/// Health check that reports whether the RabbitMQ transport can reach the broker.
/// </summary>
internal sealed class RabbitMqHealthCheck(IMessagingTransport transport) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transport is not RabbitMqTransport rabbitMq)
            return HealthCheckResult.Unhealthy("The registered messaging transport is not RabbitMQ.");

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
