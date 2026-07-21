using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sqs;

/// <summary>
/// Health check that reports whether the SQS transport can reach the queue service.
/// </summary>
internal sealed class SqsHealthCheck(IMessagingTransport transport) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transport is not SqsTransport sqs)
            return HealthCheckResult.Unhealthy("The registered messaging transport is not SQS.");

        try
        {
            await sqs.PingAsync(cancellationToken);
            return HealthCheckResult.Healthy("SQS endpoint is reachable.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Failed to reach SQS.", ex);
        }
    }
}
