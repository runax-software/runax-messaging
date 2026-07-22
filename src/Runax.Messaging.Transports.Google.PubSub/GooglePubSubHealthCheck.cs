using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Google.PubSub;

/// <summary>
/// Health check that reports whether the Google Pub/Sub transport can reach the service.
/// </summary>
internal sealed class GooglePubSubHealthCheck(IEnumerable<IMessagingTransport> transports) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (transports.OfType<GooglePubSubTransport>().FirstOrDefault() is not { } pubSub)
            return HealthCheckResult.Unhealthy("The registered messaging transport is not Google Pub/Sub.");

        try
        {
            await pubSub.PingAsync(cancellationToken);
            return HealthCheckResult.Healthy("Google Pub/Sub is reachable.");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Failed to reach Google Pub/Sub.", ex);
        }
    }
}
