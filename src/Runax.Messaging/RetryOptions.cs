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
    /// Gets or sets the suffix appended to the original topic to form the dead-letter topic. Defaults to ".dead-letter".
    /// </summary>
    public string DeadLetterTopicSuffix { get; set; } = ".dead-letter";
}
