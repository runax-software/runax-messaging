namespace Runax.Messaging.Sqs;

/// <summary>
/// Configuration options for the Amazon SQS messaging transport.
/// </summary>
public sealed class SqsOptions
{
    /// <summary>
    /// Gets or sets the AWS region. Defaults to "us-east-1".
    /// </summary>
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
    /// Gets or sets the maximum number of messages to receive per poll. Defaults to 10.
    /// </summary>
    public int MaxNumberOfMessages { get; set; } = 10;

    /// <summary>
    /// Gets or sets the long-polling wait time in seconds. Defaults to 20.
    /// </summary>
    public int WaitTimeSeconds { get; set; } = 20;

    /// <summary>
    /// Gets or sets a mapping from topic names to SQS queue URLs.
    /// If not provided, topics are used as queue names and resolved via GetQueueUrl.
    /// </summary>
    public Dictionary<string, string> TopicQueueUrlMap { get; set; } = new();
}
