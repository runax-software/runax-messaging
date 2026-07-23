using System.Collections.Concurrent;
using System.Threading.Channels;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.InMemory;

/// <summary>
/// In-process <see cref="IMessagingTransport"/> backed by unbounded channels, one per topic.
/// Useful for tests and single-process scenarios.
/// </summary>
internal sealed class InMemoryTransport : IMessagingTransport
{
    private readonly ConcurrentDictionary<string, Channel<string>> _topics = new();

    internal const string TransportName = "in-memory";

    public string SystemName => TransportName;

    private Channel<string> GetChannel(string topic)
        => _topics.GetOrAdd(topic, static _ => Channel.CreateUnbounded<string>());

    public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
        => GetChannel(topic).Writer.WriteAsync(envelopeJson, cancellationToken);

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
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
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken)
    {
        var channel = GetChannel(topic);

        await foreach (var envelopeJson in channel.Reader.ReadAllAsync(cancellationToken))
        {
            var disposition = await onMessage(envelopeJson, topic);

            // No native dead-letter facility in-process: Requeue redelivers, everything else
            // (Acknowledge / DeadLetter) drops the message from the channel.
            if (disposition == MessageDisposition.Requeue)
                await channel.Writer.WriteAsync(envelopeJson, cancellationToken);
        }
    }
}
