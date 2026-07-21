using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Sqs;

/// <summary>
/// Amazon SQS implementation of <see cref="IMessagingTransport"/>. Topics map to SQS queue names or URLs.
/// </summary>
internal sealed class SqsTransport : IMessagingTransport, IDisposable
{
    private readonly SqsOptions _options;
    private readonly ILogger<SqsTransport> _logger;
    private readonly Lazy<AmazonSQSClient> _client;
    private readonly Dictionary<string, string> _resolvedQueueUrls = new();

    public SqsTransport(SqsOptions options, ILogger<SqsTransport> logger)
    {
        _options = options;
        _logger = logger;
        _client = new Lazy<AmazonSQSClient>(() =>
        {
            var config = new AmazonSQSConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region)
            };

            if (!string.IsNullOrEmpty(_options.ServiceUrl))
                config.ServiceURL = _options.ServiceUrl;

            if (!string.IsNullOrEmpty(_options.AccessKey) && !string.IsNullOrEmpty(_options.SecretKey))
                return new AmazonSQSClient(new BasicAWSCredentials(_options.AccessKey, _options.SecretKey), config);

            return new AmazonSQSClient(config);
        });
    }

    public async ValueTask PublishAsync(
        string topic,
        string envelopeJson,
        CancellationToken cancellationToken = default)
    {
        var queueUrl = await ResolveQueueUrlAsync(topic, cancellationToken);

        await _client.Value.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = envelopeJson
        }, cancellationToken);
    }

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        var topicQueueMap = new Dictionary<string, string>();

        foreach (var topic in topics)
        {
            topicQueueMap[topic] = await ResolveQueueUrlAsync(topic, cancellationToken);
        }

        _logger.LogInformation("SQS consumer started, polling {Count} queue(s)", topicQueueMap.Count);

        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var (topic, queueUrl) in topicQueueMap)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                try
                {
                    var response = await _client.Value.ReceiveMessageAsync(new ReceiveMessageRequest
                    {
                        QueueUrl = queueUrl,
                        MaxNumberOfMessages = _options.MaxNumberOfMessages,
                        WaitTimeSeconds = _options.WaitTimeSeconds,
                        VisibilityTimeout = _options.VisibilityTimeoutSeconds
                    }, cancellationToken);

                    foreach (var message in (response.Messages ?? []).TakeWhile(_ =>
                                 !cancellationToken.IsCancellationRequested))
                    {
                        var disposition =
                            await ProcessMessageAsync(message, topic, queueUrl, onMessage, cancellationToken);

                        // Acknowledge deletes the message. Requeue and DeadLetter both leave it so the
                        // visibility timeout lapses and SQS redelivers it — and, once maxReceiveCount is hit,
                        // routes it to any configured redrive DLQ.
                        if (disposition == MessageDisposition.Acknowledge)
                        {
                            await _client.Value.DeleteMessageAsync(new DeleteMessageRequest
                            {
                                QueueUrl = queueUrl,
                                ReceiptHandle = message.ReceiptHandle
                            }, cancellationToken);
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
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
            }
        }

        _logger.LogInformation("SQS consumer shutting down");
    }

    private async ValueTask<MessageDisposition> ProcessMessageAsync(
        Message message,
        string topic,
        string queueUrl,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken)
    {
        // Keep renewing the message's visibility while the pipeline (including in-process retry backoff)
        // works on it, so it does not reappear on the queue and get processed twice.
        using var processing = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var heartbeat = _options.ExtendVisibilityDuringProcessing
            ? ExtendVisibilityAsync(queueUrl, message.ReceiptHandle, processing.Token)
            : Task.CompletedTask;

        try
        {
            return await onMessage(message.Body, topic);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Unexpected error dispatching SQS message from {QueueUrl}; leaving for redelivery.",
                queueUrl);
            return MessageDisposition.Requeue;
        }
        finally
        {
            await processing.CancelAsync();
            await heartbeat;
        }
    }

    private async Task ExtendVisibilityAsync(string queueUrl, string receiptHandle, CancellationToken cancellationToken)
    {
        // Renew at half the window so a slow consumer keeps ownership well before the timeout closes.
        var interval = TimeSpan.FromSeconds(Math.Max(1, _options.VisibilityTimeoutSeconds / 2));

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(interval, cancellationToken);

                await _client.Value.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                {
                    QueueUrl = queueUrl,
                    ReceiptHandle = receiptHandle,
                    VisibilityTimeout = _options.VisibilityTimeoutSeconds
                }, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Processing finished; stop renewing.
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to extend visibility for a message on {QueueUrl}.", queueUrl);
        }
    }

    private async ValueTask<string> ResolveQueueUrlAsync(string topic, CancellationToken cancellationToken)
    {
        if (_resolvedQueueUrls.TryGetValue(topic, out var cached)) return cached;

        if (_options.TopicQueueUrlMap.TryGetValue(topic, out var configured))
        {
            _resolvedQueueUrls[topic] = configured;
            return configured;
        }

        var response = await _client.Value.GetQueueUrlAsync(topic, cancellationToken);
        _resolvedQueueUrls[topic] = response.QueueUrl;

        return response.QueueUrl;
    }

    public void Dispose()
    {
        if (_client.IsValueCreated) _client.Value.Dispose();
    }
}
