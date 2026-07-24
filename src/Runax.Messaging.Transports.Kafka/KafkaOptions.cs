using System.ComponentModel.DataAnnotations;

namespace Runax.Messaging.Transports.Kafka;

/// <summary>
/// Configuration options for the Apache Kafka messaging transport.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>
    /// Gets or sets the comma-separated list of <c>host:port</c> bootstrap servers (e.g. "localhost:9092"). Required.
    /// </summary>
    [Required]
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the consumer group id used when subscribing. Members of the same group share the topic
    /// partitions, so offsets are committed per group. Defaults to "runax".
    /// </summary>
    [Required]
    public string ConsumerGroupId { get; set; } = "runax";

    /// <summary>
    /// Gets or sets where a new consumer group starts reading when it has no committed offset:
    /// <c>"earliest"</c> (from the beginning) or <c>"latest"</c> (only new messages). Defaults to "earliest".
    /// </summary>
    public string AutoOffsetReset { get; set; } = "earliest";

    /// <summary>
    /// Gets or sets the SASL/SSL security protocol name (e.g. <c>"SaslSsl"</c>, <c>"Ssl"</c>). When
    /// <see langword="null"/>, the client defaults to plaintext. Defaults to <see langword="null"/>.
    /// </summary>
    public string? SecurityProtocol { get; set; }

    /// <summary>
    /// Gets or sets the SASL mechanism (e.g. <c>"Plain"</c>, <c>"ScramSha256"</c>) used when
    /// <see cref="SecurityProtocol"/> selects a SASL protocol. Defaults to <see langword="null"/>.
    /// </summary>
    public string? SaslMechanism { get; set; }

    /// <summary>
    /// Gets or sets the SASL username. Defaults to <see langword="null"/>.
    /// </summary>
    public string? SaslUsername { get; set; }

    /// <summary>
    /// Gets or sets the SASL password. Prefer a secret store over a plaintext value. Defaults to <see langword="null"/>.
    /// </summary>
    public string? SaslPassword { get; set; }

    /// <summary>
    /// Gets or sets the producer acknowledgement level: <c>"all"</c> (wait for all in-sync replicas),
    /// <c>"leader"</c>, or <c>"none"</c>. Defaults to "all".
    /// </summary>
    public string Acks { get; set; } = "all";

    /// <summary>
    /// Gets or sets a value indicating whether idempotent production is enabled so retries do not create
    /// duplicates. Defaults to <see langword="true"/>.
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;

    /// <summary>
    /// Gets or sets the suffix appended to a topic name to form its dead-letter topic. A message that the
    /// dispatch pipeline dead-letters is produced to <c>{topic}{DeadLetterTopicSuffix}</c> and then its offset
    /// is committed. Defaults to ".dead-letter".
    /// </summary>
    public string DeadLetterTopicSuffix { get; set; } = ".dead-letter";

    /// <summary>
    /// Gets or sets how long a single consumer poll blocks waiting for a message before looping. Defaults to 1 second.
    /// </summary>
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(1);
}
