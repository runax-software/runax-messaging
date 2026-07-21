using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Google.PubSub;

/// <summary>
/// Health check registration extensions for the Google Cloud Pub/Sub transport.
/// </summary>
public static class GooglePubSubHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check that verifies the Google Pub/Sub service is reachable.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check name. Defaults to "google-pubsub".</param>
    /// <param name="failureStatus">The status reported on failure. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Optional tags used to filter the check.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static IHealthChecksBuilder AddGooglePubSubTransport(
        this IHealthChecksBuilder builder,
        string name = "google-pubsub",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
        => builder.Add(new HealthCheckRegistration(
            name,
            sp => new GooglePubSubHealthCheck(sp.GetRequiredService<IMessagingTransport>()),
            failureStatus,
            tags));
}
