using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Redis;

/// <summary>
/// Transport config for Redis Streams (works with Redis and Valkey). Attach to a bus with
/// <c>bus.AddTransport(new RedisConfig { Configuration = ... })</c> (or the delegate /
/// <c>IConfiguration</c>-binding <c>AddTransport</c> overloads). Registers a health check named
/// <c>runax:{bus}</c> unless <see cref="TransportConfig.RegisterHealthCheck"/> is disabled.
/// </summary>
public sealed class RedisConfig : TransportConfig
{
    /// <inheritdoc />
    public override string SystemName => RedisTransport.TransportName;

    /// <inheritdoc />
    protected override IMessagingTransport CreateTransport(TransportContext context) =>
        new RedisTransport(this, context.Services.GetRequiredService<ILogger<RedisTransport>>());

    /// <inheritdoc />
    protected override void ConfigureServices(IServiceCollection services, string busName)
    {
        if (!RegisterHealthCheck)
            return;

        services.AddHealthChecks().Add(new HealthCheckRegistration(
            $"runax:{busName}",
            sp => new RedisHealthCheck(sp.GetRequiredKeyedService<IMessagingTransport>(busName)),
            failureStatus: null,
            tags: null));
    }

    /// <summary>
    /// Gets or sets the StackExchange.Redis connection string (e.g. "localhost:6379"). Required.
    /// </summary>
    [Required]
    public string Configuration { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the consumer group used to read each stream. Defaults to "runax".
    /// </summary>
    public string ConsumerGroup { get; set; } = "runax";

    /// <summary>
    /// Gets or sets this consumer's name within the group. Defaults to a per-process unique value.
    /// </summary>
    public string ConsumerName { get; set; } = $"{Environment.MachineName}-{Guid.NewGuid():N}";

    /// <summary>
    /// Gets or sets the maximum number of entries read from a stream per poll. Defaults to 10.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int ReadBatchSize { get; set; } = 10;

    /// <summary>
    /// Gets or sets how long to wait before polling again when a stream has no new or reclaimable entries.
    /// Defaults to 1 second.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets how long a pending (unacknowledged) entry must be idle before it is reclaimed and
    /// redelivered (crash recovery, and redelivery of requeued messages). Defaults to 30 seconds.
    /// </summary>
    public TimeSpan ClaimIdleTime { get; set; } = TimeSpan.FromSeconds(30);
}
