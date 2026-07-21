namespace Runax.Messaging;

/// <summary>
/// Controls how the dispatch pipeline retries failing consumers and dead-letters
/// messages that cannot be processed.
/// </summary>
public sealed class RetryOptions
{
    /// <summary>
    /// Gets or sets the maximum number of times a consumer is invoked for a single message
    /// (initial attempt plus retries). Defaults to 3.
    /// </summary>
    public int MaxAttempts { get; set; } = 3;

    /// <summary>
    /// Gets or sets the delay before the first retry. Subsequent delays grow by
    /// <see cref="BackoffFactor"/>. Defaults to 1 second.
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the exponential multiplier applied to the delay between retries. Defaults to 2.0.
    /// </summary>
    public double BackoffFactor { get; set; } = 2.0;

    /// <summary>
    /// Gets or sets the upper bound on the delay between retries. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets a value indicating whether exhausted or poison messages are republished to a
    /// dead-letter topic. When <see langword="false"/>, such messages are logged and dropped. Defaults to <see langword="true"/>.
    /// </summary>
    public bool EnableDeadLettering { get; set; } = true;

    /// <summary>
    /// Gets or sets the suffix appended to the original topic to form the dead-letter topic when
    /// <see cref="Strategy"/> is <see cref="DeadLetterStrategy.FrameworkManaged"/>. Defaults to ".dead-letter".
    /// </summary>
    public string DeadLetterTopicSuffix { get; set; } = ".dead-letter";

    /// <summary>
    /// Gets or sets how exhausted or poison messages are dead-lettered. Defaults to
    /// <see cref="DeadLetterStrategy.FrameworkManaged"/>. Use <see cref="DeadLetterStrategy.BrokerNative"/>
    /// to defer to a broker-configured dead-letter exchange or redrive policy; pair it with
    /// <see cref="MaxAttempts"/> of 1 to rely purely on the broker for retries.
    /// </summary>
    public DeadLetterStrategy Strategy { get; set; } = DeadLetterStrategy.FrameworkManaged;
}
