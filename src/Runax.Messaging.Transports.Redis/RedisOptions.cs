using System.ComponentModel.DataAnnotations;

namespace Runax.Messaging.Transports.Redis;

/// <summary>
/// Configuration options for the Redis Streams messaging transport (works with Redis and Valkey).
/// </summary>
public sealed class RedisOptions
{
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
