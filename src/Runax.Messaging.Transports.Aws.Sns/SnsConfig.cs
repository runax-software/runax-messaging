using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sns;

/// <summary>
/// Transport config for Amazon SNS. Publishing goes to an SNS topic; consuming polls an SQS queue
/// subscribed to that topic (SNS→SQS fan-out). Attach to a bus with
/// <c>bus.AddTransport(new SnsConfig { Region = ... })</c> (or the delegate /
/// <c>IConfiguration</c>-binding <c>AddTransport</c> overloads). Registers a health check named
/// <c>runax:{bus}</c> unless <see cref="TransportConfig.RegisterHealthCheck"/> is disabled.
/// </summary>
public sealed class SnsConfig : TransportConfig
{
    /// <inheritdoc />
    public override string SystemName => SnsTransport.TransportName;

    /// <inheritdoc />
    protected override IMessagingTransport CreateTransport(TransportContext context) =>
        new SnsTransport(this, context.Services.GetRequiredService<ILogger<SnsTransport>>());

    /// <inheritdoc />
    protected override void ConfigureServices(IServiceCollection services, string busName)
    {
        if (!RegisterHealthCheck)
            return;

        services.AddHealthChecks().Add(new HealthCheckRegistration(
            $"runax:{busName}",
            sp => new SnsHealthCheck(sp.GetRequiredKeyedService<IMessagingTransport>(busName)),
            failureStatus: null,
            tags: null));
    }

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
    /// Gets or sets a mapping from topic name to SNS topic ARN for publishing.
    /// If a topic is not mapped, its ARN is resolved by creating the topic (idempotent).
    /// </summary>
    // ReSharper disable once CollectionNeverUpdated.Global — populated by consumers via the configure action.
    public Dictionary<string, string> TopicArnMap { get; set; } = new();

    /// <summary>
    /// Gets or sets a mapping from topic name to the SQS queue URL (subscribed to the SNS topic) used to
    /// consume it. A topic must have an entry here to be consumable.
    /// </summary>
    // ReSharper disable once CollectionNeverUpdated.Global — populated by consumers via the configure action.
    public Dictionary<string, string> TopicQueueUrlMap { get; set; } = new();

    /// <summary>
    /// Gets or sets the maximum number of messages to receive per SQS poll (SQS allows 1–10). Defaults to 10.
    /// </summary>
    [Range(1, 10)]
    public int MaxNumberOfMessages { get; set; } = 10;

    /// <summary>
    /// Gets or sets the SQS long-polling wait time in seconds (SQS allows 0–20). Defaults to 20.
    /// </summary>
    [Range(0, 20)]
    public int WaitTimeSeconds { get; set; } = 20;

    /// <summary>
    /// Gets or sets the SQS visibility timeout, in seconds, requested when receiving a message (SQS allows 0–43200).
    /// Defaults to 30.
    /// </summary>
    [Range(0, 43200)]
    public int VisibilityTimeoutSeconds { get; set; } = 30;
}
