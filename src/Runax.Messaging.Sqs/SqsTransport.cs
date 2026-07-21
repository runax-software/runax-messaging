using System.Collections.Concurrent;
using System.Globalization;
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
    private readonly ConcurrentDictionary<string, string> _resolvedQueueUrls = new();

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

    public string SystemName => "sqs";

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

    public async ValueTask PublishBatchAsync(
        string topic,
        IReadOnlyList<string> envelopeJsons,
        CancellationToken cancellationToken = default)
    {
        if (envelopeJsons.Count == 0)
            return;

        var queueUrl = await ResolveQueueUrlAsync(topic, cancellationToken);

        // SendMessageBatch accepts at most 10 entries per call.
        for (var offset = 0; offset < envelopeJsons.Count; offset += 10)
        {
            var entries = new List<SendMessageBatchRequestEntry>(Math.Min(10, envelopeJsons.Count - offset));
            for (var i = offset; i < offset + 10 && i < envelopeJsons.Count; i++)
            {
                entries.Add(new SendMessageBatchRequestEntry
                {
                    Id = (i - offset).ToString(CultureInfo.InvariantCulture),
                    MessageBody = envelopeJsons[i]
                });
            }

            var response = await _client.Value.SendMessageBatchAsync(new SendMessageBatchRequest
            {
                QueueUrl = queueUrl,
                Entries = entries
            }, cancellationToken);

            if (response.Failed is { Count: > 0 })
            {
                throw new InvalidOperationException(
                    $"SQS batch publish to '{topic}' failed for {response.Failed.Count} message(s): " +
                    string.Join("; ", response.Failed.Select(f => $"{f.Id}:{f.Code}")));
            }
        }
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

        // One permit per allowed in-flight handler, shared across every queue pump.
        using var concurrencyLimiter = new SemaphoreSlim(Math.Max(1, _options.MaxConcurrentMessages));

        _logger.LogInformation("SQS consumer started, polling {Count} queue(s) with up to {Concurrency} concurrent message(s)",
            topicQueueMap.Count, _options.MaxConcurrentMessages);

        var pumps = topicQueueMap.Select(entry =>
            PumpQueueAsync(entry.Key, entry.Value, onMessage, concurrencyLimiter, cancellationToken));

        try
        {
            await Task.WhenAll(pumps);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Graceful shutdown.
        }

        _logger.LogInformation("SQS consumer shutting down");
    }

    private async Task PumpQueueAsync(
        string topic,
        string queueUrl,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        SemaphoreSlim concurrencyLimiter,
        CancellationToken cancellationToken)
    {
        var inFlight = new List<Task>();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var response = await _client.Value.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = queueUrl,
                    MaxNumberOfMessages = _options.MaxNumberOfMessages,
                    WaitTimeSeconds = _options.WaitTimeSeconds,
                    VisibilityTimeout = _options.VisibilityTimeoutSeconds
                }, cancellationToken);

                foreach (var message in response.Messages ?? [])
                {
                    // Acquire a slot before starting the handler; this backpressures polling so no more
                    // than MaxConcurrentMessages are handled at once, and lets successive polls overlap.
                    await concurrencyLimiter.WaitAsync(cancellationToken);
                    inFlight.Add(RunHandlerAsync(message, topic, queueUrl, onMessage, concurrencyLimiter, cancellationToken));
                }

                inFlight.RemoveAll(task => task.IsCompleted);
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

        await Task.WhenAll(inFlight);
    }

    private async Task RunHandlerAsync(
        Message message,
        string topic,
        string queueUrl,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        SemaphoreSlim concurrencyLimiter,
        CancellationToken cancellationToken)
    {
        try
        {
            var disposition = await ProcessMessageAsync(message, topic, queueUrl, onMessage, cancellationToken);

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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown; the message stays on the queue for redelivery.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error handling SQS message from {QueueUrl}", queueUrl);
        }
        finally
        {
            concurrencyLimiter.Release();
        }
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

    /// <summary>
    /// Verifies broker reachability with a lightweight <c>ListQueues</c> call against the configured endpoint.
    /// </summary>
    internal async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        await _client.Value.ListQueuesAsync(new ListQueuesRequest { MaxResults = 1 }, cancellationToken);
        return true;
    }

    public void Dispose()
    {
        if (_client.IsValueCreated) _client.Value.Dispose();
    }
}
