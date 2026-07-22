using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sns;

/// <summary>
/// Health check registration extensions for the Amazon SNS transport.
/// </summary>
public static class SnsHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check that verifies the SNS endpoint is reachable.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check name. Defaults to "sns".</param>
    /// <param name="failureStatus">The status reported on failure. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Optional tags used to filter the check.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static IHealthChecksBuilder AddSnsTransport(
        this IHealthChecksBuilder builder,
        string name = "sns",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
        => builder.Add(new HealthCheckRegistration(
            name,
            sp => new SnsHealthCheck(sp.GetServices<IMessagingTransport>()),
            failureStatus,
            tags));
}
