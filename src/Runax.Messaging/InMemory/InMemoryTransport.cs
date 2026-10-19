using System.Collections.Concurrent;
using System.Threading.Channels;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.InMemory;

/// <summary>
/// In-process <see cref="IMessagingTransport"/> backed by unbounded channels, one per topic.
/// Useful for tests and single-process scenarios. Native headers travel with each message.
/// </summary>
internal sealed class InMemoryTransport : IMessagingTransport
{
    private readonly ConcurrentDictionary<string, Channel<TransportMessage>> _topics = new();

    internal const string TransportName = "in-memory";

    public string SystemName => TransportName;

    private Channel<TransportMessage> GetChannel(string topic)
        => _topics.GetOrAdd(topic, static _ => Channel.CreateUnbounded<TransportMessage>());

    public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
        => PublishAsync(new TransportMessage(topic, envelopeJson), cancellationToken);

    public ValueTask PublishAsync(TransportMessage message, CancellationToken cancellationToken = default)
        => GetChannel(message.Topic).Writer.WriteAsync(message, cancellationToken);

    public async ValueTask PublishBatchAsync(
        IReadOnlyList<TransportMessage> messages,
        CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
            await PublishAsync(message, cancellationToken).ConfigureAwait(false);
    }

    public Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
        => SubscribeAsync(topics, message => onMessage(message.Body, message.Topic), cancellationToken);

    public async Task SubscribeAsync(
        string[] topics,
        Func<TransportMessage, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        var pumps = topics.Select(topic => PumpAsync(topic, onMessage, cancellationToken));

        try
        {
            await Task.WhenAll(pumps);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Graceful shutdown.
        }
    }

    private async Task PumpAsync(
        string topic,
        Func<TransportMessage, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken)
    {
        var channel = GetChannel(topic);

        await foreach (var message in channel.Reader.ReadAllAsync(cancellationToken))
        {
            var disposition = await onMessage(message);

            // No native dead-letter facility in-process: Requeue redelivers, everything else
            // (Acknowledge / DeadLetter) drops the message from the channel.
            if (disposition == MessageDisposition.Requeue)
                await channel.Writer.WriteAsync(message, cancellationToken);
        }
    }
}
