using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.ServiceBus;

/// <summary>
/// Health check registration extensions for the Azure Service Bus transport.
/// </summary>
public static class AzureServiceBusHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check that verifies the Service Bus namespace is reachable.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check name. Defaults to "azure-servicebus".</param>
    /// <param name="failureStatus">The status reported on failure. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Optional tags used to filter the check.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static IHealthChecksBuilder AddAzureServiceBusTransport(
        this IHealthChecksBuilder builder,
        string name = "azure-servicebus",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
        => builder.Add(new HealthCheckRegistration(
            name,
            sp => new AzureServiceBusHealthCheck(sp.GetServices<IMessagingTransport>()),
            failureStatus,
            tags));
}
