using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.EventHubs;

/// <summary>
/// Health check registration extensions for the Azure Event Hubs transport.
/// </summary>
public static class AzureEventHubsHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check that verifies the Event Hubs namespace is reachable by fetching properties
    /// for the given probe event hub.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="eventHub">The event hub (runax topic) used as the connectivity probe.</param>
    /// <param name="name">The health check name. Defaults to "azure-eventhubs".</param>
    /// <param name="failureStatus">The status reported on failure. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Optional tags used to filter the check.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static IHealthChecksBuilder AddAzureEventHubsTransport(
        this IHealthChecksBuilder builder,
        string eventHub,
        string name = "azure-eventhubs",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
        => builder.Add(new HealthCheckRegistration(
            name,
            sp => new AzureEventHubsHealthCheck(sp.GetServices<IMessagingTransport>(), eventHub),
            failureStatus,
            tags));
}
