using System.ComponentModel.DataAnnotations;

namespace Runax.Messaging.Sqs;

/// <summary>
/// Configuration options for the Amazon SQS messaging transport.
/// </summary>
public sealed class SqsOptions
{
    /// <summary>
    /// Gets or sets the AWS region. Defaults to "us-east-1".
    /// </summary>
    [Required]
    public string Region { get; set; } = "us-east-1";

    /// <summary>
    /// Gets or sets the AWS access key. If null, the default credential chain is used.
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>
    /// Gets or sets the AWS secret key. If null, the default credential chain is used.
    /// </summary>
    public string? SecretKey { get; set; }

    /// <summary>
    /// Gets or sets a custom service URL (for LocalStack or testing). If null, the default AWS endpoint is used.
    /// </summary>
    public string? ServiceUrl { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of messages to receive per poll (SQS allows 1–10). Defaults to 10.
    /// </summary>
    [Range(1, 10)]
    public int MaxNumberOfMessages { get; set; } = 10;

    /// <summary>
    /// Gets or sets the long-polling wait time in seconds (SQS allows 0–20). Defaults to 20.
    /// </summary>
    [Range(0, 20)]
    public int WaitTimeSeconds { get; set; } = 20;

    /// <summary>
    /// Gets or sets the maximum number of messages processed concurrently across all polled queues.
    /// Each queue is polled by its own pump; this bounds the total in-flight handlers. Defaults to 10.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxConcurrentMessages { get; set; } = 10;

    /// <summary>
    /// Gets or sets a mapping from topic names to SQS queue URLs.
    /// If not provided, topics are used as queue names and resolved via GetQueueUrl.
    /// </summary>
    // ReSharper disable once CollectionNeverUpdated.Global — populated by consumers via the configure action.
    public Dictionary<string, string> TopicQueueUrlMap { get; set; } = new();

    /// <summary>
    /// Gets or sets the visibility timeout, in seconds, requested when receiving a message (SQS allows 0–43200).
    /// This hides the message from other consumers while it is being processed. Defaults to 30.
    /// </summary>
    [Range(0, 43200)]
    public int VisibilityTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Gets or sets a value indicating whether the transport periodically extends a message's visibility timeout
    /// while it is being processed, so in-process retry backoff does not let the message reappear on the queue.
    /// Defaults to <see langword="true"/>.
    /// </summary>
    public bool ExtendVisibilityDuringProcessing { get; set; } = true;
}
