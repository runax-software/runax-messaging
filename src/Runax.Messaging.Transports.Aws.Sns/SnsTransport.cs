using System.Collections.Concurrent;
using System.Text.Json;
using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sns;

/// <summary>
/// Amazon SNS implementation of <see cref="IMessagingTransport"/>. Publishing goes to an SNS topic;
/// consuming polls an SQS queue subscribed to that topic and unwraps the SNS notification envelope.
/// </summary>
internal sealed class SnsTransport : IMessagingTransport, IDisposable
{
    private readonly SnsOptions _options;
    private readonly ILogger<SnsTransport> _logger;
    private readonly Lazy<AmazonSimpleNotificationServiceClient> _sns;
    private readonly Lazy<AmazonSQSClient> _sqs;
    private readonly ConcurrentDictionary<string, string> _topicArns = new();

    public SnsTransport(SnsOptions options, ILogger<SnsTransport> logger)
    {
        _options = options;
        _logger = logger;

        var region = RegionEndpoint.GetBySystemName(options.Region);
        _sns = new Lazy<AmazonSimpleNotificationServiceClient>(() =>
        {
            var config = new AmazonSimpleNotificationServiceConfig { RegionEndpoint = region };
            if (!string.IsNullOrEmpty(_options.ServiceUrl))
                config.ServiceURL = _options.ServiceUrl;
            return HasStaticCredentials
                ? new AmazonSimpleNotificationServiceClient(Credentials, config)
                : new AmazonSimpleNotificationServiceClient(config);
        });
        _sqs = new Lazy<AmazonSQSClient>(() =>
        {
            var config = new AmazonSQSConfig { RegionEndpoint = region };
            if (!string.IsNullOrEmpty(_options.ServiceUrl))
                config.ServiceURL = _options.ServiceUrl;
            return HasStaticCredentials
                ? new AmazonSQSClient(Credentials, config)
                : new AmazonSQSClient(config);
        });
    }

    public string SystemName => "aws_sns";

    private bool HasStaticCredentials => !string.IsNullOrEmpty(_options.AccessKey) && !string.IsNullOrEmpty(_options.SecretKey);

    private AWSCredentials Credentials => new BasicAWSCredentials(_options.AccessKey, _options.SecretKey);

    public async ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
    {
        var topicArn = await ResolveTopicArnAsync(topic, cancellationToken).ConfigureAwait(false);
        await _sns.Value.PublishAsync(topicArn, envelopeJson, cancellationToken).ConfigureAwait(false);
    }

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        var pumps = new List<Task>();
        foreach (var topic in topics)
        {
            if (!_options.TopicQueueUrlMap.TryGetValue(topic, out var queueUrl))
            {
                _logger.LogWarning(
                    "No SQS queue mapped for topic '{Topic}'; it cannot be consumed. Add it to TopicQueueUrlMap.", topic);
                continue;
            }

            pumps.Add(PumpQueueAsync(topic, queueUrl, onMessage, cancellationToken));
        }

        _logger.LogInformation("SNS consumer started, polling {Count} subscribed queue(s)", pumps.Count);

        try
        {
            await Task.WhenAll(pumps).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Graceful shutdown.
        }

        _logger.LogInformation("SNS consumer shutting down");
    }

    private async Task PumpQueueAsync(
        string topic,
        string queueUrl,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var response = await _sqs.Value.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = queueUrl,
                    MaxNumberOfMessages = _options.MaxNumberOfMessages,
                    WaitTimeSeconds = _options.WaitTimeSeconds,
                    VisibilityTimeout = _options.VisibilityTimeoutSeconds
                }, cancellationToken).ConfigureAwait(false);

                foreach (var message in (response.Messages ?? []).TakeWhile(_ => !cancellationToken.IsCancellationRequested))
                {
                    MessageDisposition disposition;
                    try
                    {
                        disposition = await onMessage(UnwrapSnsBody(message.Body), topic).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unexpected error dispatching SNS message from {QueueUrl}; leaving for redelivery.", queueUrl);
                        disposition = MessageDisposition.Requeue;
                    }

                    // Acknowledge deletes; Requeue and DeadLetter leave the message so its visibility timeout
                    // lapses and SQS redelivers it (and routes to a redrive DLQ once maxReceiveCount is hit).
                    if (disposition == MessageDisposition.Acknowledge)
                    {
                        await _sqs.Value.DeleteMessageAsync(new DeleteMessageRequest
                        {
                            QueueUrl = queueUrl,
                            ReceiptHandle = message.ReceiptHandle
                        }, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error polling SQS queue {QueueUrl}", queueUrl);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // SNS→SQS without raw message delivery wraps the payload in a notification envelope; unwrap it.
    // With raw delivery (or an already-unwrapped body) the body is returned unchanged.
    private static string UnwrapSnsBody(string body)
    {
        try
        {
            using var document = JsonDocument.Parse(body);
            if (document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty("Type", out var type)
                && type.ValueKind == JsonValueKind.String && type.GetString() == "Notification"
                && document.RootElement.TryGetProperty("Message", out var message)
                && message.ValueKind == JsonValueKind.String)
            {
                return message.GetString()!;
            }
        }
        catch (JsonException)
        {
            // Not JSON, so not an SNS envelope.
        }

        return body;
    }

    private async ValueTask<string> ResolveTopicArnAsync(string topic, CancellationToken cancellationToken)
    {
        if (_topicArns.TryGetValue(topic, out var cached))
            return cached;

        if (_options.TopicArnMap.TryGetValue(topic, out var configured))
        {
            _topicArns[topic] = configured;
            return configured;
        }

        // CreateTopic is idempotent and returns the ARN for an existing topic.
        var response = await _sns.Value.CreateTopicAsync(topic, cancellationToken).ConfigureAwait(false);
        _topicArns[topic] = response.TopicArn;
        return response.TopicArn;
    }

    /// <summary>
    /// Verifies reachability with a lightweight <c>ListTopics</c> call against SNS.
    /// </summary>
    internal async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        await _sns.Value.ListTopicsAsync(new ListTopicsRequest(), cancellationToken).ConfigureAwait(false);
        return true;
    }

    public void Dispose()
    {
        if (_sns.IsValueCreated) _sns.Value.Dispose();
        if (_sqs.IsValueCreated) _sqs.Value.Dispose();
    }
}
