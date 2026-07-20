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
                        WaitTimeSeconds = _options.WaitTimeSeconds
                    }, cancellationToken);

                    foreach (var message in (response.Messages ?? []).TakeWhile(message =>
                                 !cancellationToken.IsCancellationRequested))
                    {
                        MessageDisposition disposition;
                        try
                        {
                            disposition = await onMessage(message.Body, topic);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex,
                                "Unexpected error dispatching SQS message from {QueueUrl}; leaving for redelivery.",
                                queueUrl);
                            disposition = MessageDisposition.Requeue;
                        }

                        // Requeue: leave the message so its visibility timeout expires and SQS redelivers it
                        // (and, once maxReceiveCount is hit, routes it to any configured redrive DLQ).
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

    private async ValueTask<string> ResolveQueueUrlAsync(string topic, CancellationToken cancellationToken)
    {
        if (_resolvedQueueUrls.TryGetValue(topic, out var cached))
            return cached;

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
        if (_client.IsValueCreated)
            _client.Value.Dispose();
    }
}
