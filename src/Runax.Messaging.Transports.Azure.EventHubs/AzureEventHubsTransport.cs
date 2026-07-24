using System.Collections.Concurrent;
using System.Text;
using global::Azure.Identity;
using global::Azure.Messaging.EventHubs;
using global::Azure.Messaging.EventHubs.Processor;
using global::Azure.Messaging.EventHubs.Producer;
using global::Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.EventHubs;

/// <summary>
/// Azure Event Hubs implementation of <see cref="IMessagingTransport"/>. A runax topic maps to an event
/// hub of the same name. Publishing sends the envelope through an <see cref="EventHubProducerClient"/>;
/// consuming runs an <see cref="EventProcessorClient"/> over a consumer group backed by a blob checkpoint
/// store. Event Hubs has no per-message ack or native dead-letter queue, so the disposition is mapped as:
/// <c>Acknowledge</c> advances the checkpoint; <c>Requeue</c> skips the checkpoint so the partition is
/// reprocessed; <c>DeadLetter</c> either republishes to a <c>{topic}.dead-letter</c> hub or is logged and
/// checkpointed (dropped), controlled by <see cref="AzureEventHubsOptions.ProduceDeadLetterHub"/>.
/// </summary>
internal sealed class AzureEventHubsTransport : IMessagingTransport, IDisposable
{
    private readonly AzureEventHubsOptions _options;
    private readonly ILogger<AzureEventHubsTransport> _logger;
    private readonly ConcurrentDictionary<string, EventHubProducerClient> _producers = new();

    public AzureEventHubsTransport(AzureEventHubsOptions options, ILogger<AzureEventHubsTransport> logger)
    {
        _options = options;
        _logger = logger;
        _options.EnsureConnectionConfigured();
    }

    internal const string TransportName = "azure-event-hubs";

    public string SystemName => TransportName;

    public async ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
    {
        var producer = GetProducer(topic);
        using var batch = await producer.CreateBatchAsync(cancellationToken).ConfigureAwait(false);
        if (!batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(envelopeJson))))
            throw new InvalidOperationException($"Event for topic '{topic}' is too large for an Event Hubs batch.");

        await producer.SendAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask PublishBatchAsync(
        string topic,
        IReadOnlyList<string> envelopeJsons,
        CancellationToken cancellationToken = default)
    {
        if (envelopeJsons.Count == 0)
            return;

        var producer = GetProducer(topic);

        // Event Hubs batches are size-bounded; roll over to a new batch whenever an event does not fit.
        var batch = await producer.CreateBatchAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            foreach (var envelopeJson in envelopeJsons)
            {
                var eventData = new EventData(Encoding.UTF8.GetBytes(envelopeJson));
                if (batch.TryAdd(eventData))
                    continue;

                if (batch.Count == 0)
                    throw new InvalidOperationException($"Event for topic '{topic}' is too large for an Event Hubs batch.");

                await producer.SendAsync(batch, cancellationToken).ConfigureAwait(false);
                batch.Dispose();
                batch = await producer.CreateBatchAsync(cancellationToken).ConfigureAwait(false);

                if (!batch.TryAdd(eventData))
                    throw new InvalidOperationException($"Event for topic '{topic}' is too large for an Event Hubs batch.");
            }

            if (batch.Count > 0)
                await producer.SendAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            batch.Dispose();
        }
    }

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        _options.EnsureCheckpointStoreConfigured();

        var containerClient = new BlobContainerClient(_options.BlobConnectionString, _options.BlobContainerName);
        await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        var processors = new List<EventProcessorClient>();
        foreach (var topic in topics)
        {
            var processor = string.IsNullOrWhiteSpace(_options.ConnectionString)
                ? new EventProcessorClient(containerClient, _options.ConsumerGroup, _options.FullyQualifiedNamespace, topic, CreateCredential(), clientOptions: null)
                : new EventProcessorClient(containerClient, _options.ConsumerGroup, _options.ConnectionString, topic);

            var processorTopic = topic;
            processor.ProcessEventAsync += args => OnEventAsync(args, processorTopic, onMessage);
            processor.ProcessErrorAsync += args =>
            {
                _logger.LogError(args.Exception, "Event Hubs processor error on {Hub} partition {Partition}",
                    topic, args.PartitionId);
                return Task.CompletedTask;
            };

            await processor.StartProcessingAsync(cancellationToken).ConfigureAwait(false);
            processors.Add(processor);
            _logger.LogInformation("Subscribed to Event Hub {Topic} on consumer group {ConsumerGroup}",
                topic, _options.ConsumerGroup);
        }

        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Event Hubs consumer shutting down");
        }
        finally
        {
            foreach (var processor in processors)
            {
                try
                {
                    await processor.StopProcessingAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error stopping an Event Hubs processor.");
                }
            }
        }
    }

    private async Task OnEventAsync(
        ProcessEventArgs args,
        string topic,
        Func<string, string, ValueTask<MessageDisposition>> onMessage)
    {
        if (!args.HasEvent)
            return;

        MessageDisposition disposition;
        try
        {
            var json = Encoding.UTF8.GetString(args.Data.EventBody.ToArray());
            disposition = await onMessage(json, topic).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error dispatching Event Hubs message on '{Topic}'; not checkpointing.", topic);
            disposition = MessageDisposition.Requeue;
        }

        switch (disposition)
        {
            case MessageDisposition.Acknowledge:
                await args.UpdateCheckpointAsync(args.CancellationToken).ConfigureAwait(false);
                break;
            case MessageDisposition.DeadLetter:
                await DeadLetterAsync(args, topic).ConfigureAwait(false);
                break;
            case MessageDisposition.Requeue:
            default:
                // Do not checkpoint: the partition is reprocessed from the last committed offset on
                // the next run or by another owner, giving the event another chance.
                _logger.LogWarning("Requeue requested for Event Hubs message on '{Topic}'; skipping checkpoint.", topic);
                break;
        }
    }

    private async Task DeadLetterAsync(ProcessEventArgs args, string topic)
    {
        if (_options.ProduceDeadLetterHub)
        {
            var deadLetterTopic = topic + AzureEventHubsOptions.DeadLetterHubSuffix;
            try
            {
                var producer = GetProducer(deadLetterTopic);
                using var batch = await producer.CreateBatchAsync(args.CancellationToken).ConfigureAwait(false);
                batch.TryAdd(new EventData(args.Data.EventBody.ToArray()));
                await producer.SendAsync(batch, args.CancellationToken).ConfigureAwait(false);
                _logger.LogInformation("Dead-lettered Event Hubs message from '{Topic}' to '{DeadLetterTopic}'.",
                    topic, deadLetterTopic);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to produce dead-letter for '{Topic}'; checkpointing to avoid a poison loop.", topic);
            }
        }
        else
        {
            _logger.LogWarning(
                "Dead-letter requested for Event Hubs message on '{Topic}'; Event Hubs has no native dead-letter " +
                "queue, so the message is dropped (checkpointed). Set ProduceDeadLetterHub to republish it instead.",
                topic);
        }

        // Advance past the message in both cases so it is not reprocessed.
        await args.UpdateCheckpointAsync(args.CancellationToken).ConfigureAwait(false);
    }

    private EventHubProducerClient GetProducer(string topic) =>
        _producers.GetOrAdd(topic, hub => string.IsNullOrWhiteSpace(_options.ConnectionString)
            ? new EventHubProducerClient(_options.FullyQualifiedNamespace, hub, CreateCredential())
            : new EventHubProducerClient(_options.ConnectionString, hub));

    private static DefaultAzureCredential CreateCredential() => new();

    /// <summary>
    /// Verifies reachability by fetching event hub properties for the given topic.
    /// </summary>
    internal async Task<bool> PingAsync(string topic, CancellationToken cancellationToken = default)
    {
        var producer = GetProducer(topic);
        await producer.GetEventHubPropertiesAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    public void Dispose()
    {
        foreach (var producer in _producers.Values)
            producer.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }
}
