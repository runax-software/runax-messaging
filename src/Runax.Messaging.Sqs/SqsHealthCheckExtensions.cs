using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Sqs;

/// <summary>
/// Health check registration extensions for the Amazon SQS transport.
/// </summary>
public static class SqsHealthCheckExtensions
{
    /// <summary>
    /// Adds a health check that verifies the SQS endpoint is reachable.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check name. Defaults to "sqs".</param>
    /// <param name="failureStatus">The status reported on failure. Defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Optional tags used to filter the check.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static IHealthChecksBuilder AddSqsTransport(
        this IHealthChecksBuilder builder,
        string name = "sqs",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null)
        => builder.Add(new HealthCheckRegistration(
            name,
            sp => new SqsHealthCheck(sp.GetRequiredService<IMessagingTransport>()),
            failureStatus,
            tags));
}
