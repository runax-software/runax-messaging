using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Kafka;

/// <summary>
/// Apache Kafka implementation of <see cref="IMessagingTransport"/>. A topic maps directly to a Kafka topic;
/// publishing produces the envelope as the record value and consuming uses a group with manual offset commits.
/// Kafka has no per-message ack or native dead-letter queue, so dispositions are mapped onto offset control:
/// <c>Acknowledge</c> commits the offset, <c>Requeue</c> seeks back so the record is redelivered, and
/// <c>DeadLetter</c> produces the record to <c>{topic}{DeadLetterTopicSuffix}</c> before committing.
/// </summary>
internal sealed class KafkaTransport : IMessagingTransport, IDisposable
{
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaTransport> _logger;
    private readonly Lazy<IProducer<Null, string>> _producer;

    public KafkaTransport(KafkaOptions options, ILogger<KafkaTransport> logger)
    {
        _options = options;
        _logger = logger;
        _producer = new Lazy<IProducer<Null, string>>(
            () => new ProducerBuilder<Null, string>(BuildProducerConfig()).Build());
    }

    internal const string TransportName = "kafka";

    public string SystemName => TransportName;

    public async ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
    {
        await _producer.Value
            .ProduceAsync(topic, new Message<Null, string> { Value = envelopeJson }, cancellationToken)
            .ConfigureAwait(false);
    }

    public async ValueTask PublishBatchAsync(
        string topic,
        IReadOnlyList<string> envelopeJsons,
        CancellationToken cancellationToken = default)
    {
        if (envelopeJsons.Count == 0)
            return;

        var producer = _producer.Value;

        // Fire every produce without awaiting the per-message delivery, then flush the whole batch once.
        var pending = new List<Task<DeliveryResult<Null, string>>>(envelopeJsons.Count);
        foreach (var envelopeJson in envelopeJsons)
            pending.Add(producer.ProduceAsync(topic, new Message<Null, string> { Value = envelopeJson }, cancellationToken));

        await Task.WhenAll(pending).ConfigureAwait(false);
    }

    public Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        // Consume() is a blocking call, so run the poll loop on a dedicated background thread.
        return Task.Factory.StartNew(
            () => ConsumeLoop(topics, onMessage, cancellationToken),
            cancellationToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    private void ConsumeLoop(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken)
    {
        using var consumer = new ConsumerBuilder<Null, string>(BuildConsumerConfig()).Build();
        consumer.Subscribe(topics);

        _logger.LogInformation("Kafka consumer started in group {Group} for {Count} topic(s)",
            _options.ConsumerGroupId, topics.Length);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<Null, string>? result;
                try
                {
                    result = consumer.Consume(_options.PollTimeout);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Error consuming from Kafka");
                    continue;
                }

                if (result?.Message is null)
                    continue;

                SettleAsync(consumer, result, onMessage, cancellationToken).GetAwaiter().GetResult();
            }
        }
        finally
        {
            // Leaves the group cleanly and commits final offsets.
            consumer.Close();
            _logger.LogInformation("Kafka consumer shutting down");
        }
    }

    private async Task SettleAsync(
        IConsumer<Null, string> consumer,
        ConsumeResult<Null, string> result,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken)
    {
        MessageDisposition disposition;
        try
        {
            disposition = await onMessage(result.Message.Value, result.Topic).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error dispatching Kafka message; requeueing.");
            disposition = MessageDisposition.Requeue;
        }

        try
        {
            switch (disposition)
            {
                case MessageDisposition.Acknowledge:
                    // Advance the committed offset past this record so it is not redelivered.
                    consumer.Commit(result);
                    break;
                case MessageDisposition.DeadLetter:
                    // Kafka has no native DLQ, so produce to a companion dead-letter topic, then commit.
                    var deadLetterTopic = result.Topic + _options.DeadLetterTopicSuffix;
                    await _producer.Value.ProduceAsync(
                        deadLetterTopic,
                        new Message<Null, string> { Value = result.Message.Value },
                        cancellationToken).ConfigureAwait(false);
                    consumer.Commit(result);
                    break;
                case MessageDisposition.Requeue:
                default:
                    // Don't commit; seek back to this record so the next poll redelivers it.
                    consumer.Seek(result.TopicPartitionOffset);
                    break;
            }
        }
        catch (KafkaException ex)
        {
            _logger.LogWarning(ex, "Failed to settle Kafka message at {Offset}.", result.TopicPartitionOffset);
        }
    }

    /// <summary>
    /// Verifies broker reachability by requesting cluster metadata through an admin client.
    /// </summary>
    internal async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _options.BootstrapServers
        }).Build();

        // Metadata is fetched synchronously; run it off the caller's thread and honor cancellation.
        return await Task.Run(
            () =>
            {
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
                return metadata.Brokers.Count > 0;
            },
            cancellationToken).ConfigureAwait(false);
    }

    private ProducerConfig BuildProducerConfig()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            EnableIdempotence = _options.EnableIdempotence
        };

        if (Enum.TryParse<Acks>(_options.Acks, ignoreCase: true, out var acks))
            config.Acks = acks;

        ApplySecurity(config);
        return config;
    }

    private ConsumerConfig BuildConsumerConfig()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroupId,
            // Offsets are committed explicitly per disposition, never on a timer.
            EnableAutoCommit = false
        };

        if (Enum.TryParse<AutoOffsetReset>(_options.AutoOffsetReset, ignoreCase: true, out var offsetReset))
            config.AutoOffsetReset = offsetReset;

        ApplySecurity(config);
        return config;
    }

    private void ApplySecurity(ClientConfig config)
    {
        if (Enum.TryParse<SecurityProtocol>(_options.SecurityProtocol, ignoreCase: true, out var protocol))
            config.SecurityProtocol = protocol;

        if (Enum.TryParse<SaslMechanism>(_options.SaslMechanism, ignoreCase: true, out var mechanism))
            config.SaslMechanism = mechanism;

        if (_options.SaslUsername is not null)
            config.SaslUsername = _options.SaslUsername;

        if (_options.SaslPassword is not null)
            config.SaslPassword = _options.SaslPassword;
    }

    public void Dispose()
    {
        if (_producer.IsValueCreated)
        {
            _producer.Value.Flush(TimeSpan.FromSeconds(5));
            _producer.Value.Dispose();
        }
    }
}
