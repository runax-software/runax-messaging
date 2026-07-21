using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;
using StackExchange.Redis;

namespace Runax.Messaging.Transports.Redis;

/// <summary>
/// Redis Streams implementation of <see cref="IMessagingTransport"/> (works with Redis and Valkey).
/// A topic maps to a stream key; publishing is <c>XADD</c> and consuming uses a consumer group
/// (<c>XREADGROUP</c> for new messages, <c>XAUTOCLAIM</c> to redeliver idle-pending ones).
/// </summary>
internal sealed class RedisTransport : IMessagingTransport, IDisposable
{
    private const string DataField = "data";
    private static readonly RedisValue GroupStartPosition = "$";  // new messages after group creation
    private static readonly RedisValue UndeliveredMessages = ">"; // XREADGROUP: not-yet-delivered
    private static readonly RedisValue ClaimStartId = "0-0";

    private readonly RedisOptions _options;
    private readonly ILogger<RedisTransport> _logger;
    private readonly Lazy<Task<IConnectionMultiplexer>> _connection;

    public RedisTransport(RedisOptions options, ILogger<RedisTransport> logger)
    {
        _options = options;
        _logger = logger;
        _connection = new Lazy<Task<IConnectionMultiplexer>>(
            async () => await ConnectionMultiplexer.ConnectAsync(_options.Configuration).ConfigureAwait(false));
    }

    public string SystemName => "redis";

    public async ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
    {
        var db = await GetDatabaseAsync().ConfigureAwait(false);
        await db.StreamAddAsync(topic, DataField, envelopeJson).ConfigureAwait(false);
    }

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        var db = await GetDatabaseAsync().ConfigureAwait(false);

        foreach (var topic in topics)
            await EnsureConsumerGroupAsync(db, topic).ConfigureAwait(false);

        _logger.LogInformation("Redis consumer started for {Count} stream(s) as group {Group}/{Consumer}",
            topics.Length, _options.ConsumerGroup, _options.ConsumerName);

        var pumps = topics.Select(topic => PumpStreamAsync(db, topic, onMessage, cancellationToken));

        try
        {
            await Task.WhenAll(pumps).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Graceful shutdown.
        }

        _logger.LogInformation("Redis consumer shutting down");
    }

    private async Task EnsureConsumerGroupAsync(IDatabaseAsync db, string topic)
    {
        try
        {
            await db.StreamCreateConsumerGroupAsync(topic, _options.ConsumerGroup, GroupStartPosition, createStream: true)
                .ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.Ordinal))
        {
            // The group already exists.
        }
    }

    private async Task PumpStreamAsync(
        IDatabaseAsync db,
        string topic,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Reclaim entries idle beyond ClaimIdleTime — crashed consumers, or messages left pending
                // by a Requeue verdict — and redeliver them.
                var claim = await db.StreamAutoClaimAsync(topic, _options.ConsumerGroup, _options.ConsumerName,
                    (long)_options.ClaimIdleTime.TotalMilliseconds, ClaimStartId, _options.ReadBatchSize).ConfigureAwait(false);
                var handledClaimed = await HandleEntriesAsync(db, topic, claim.ClaimedEntries, onMessage, cancellationToken).ConfigureAwait(false);

                var entries = await db.StreamReadGroupAsync(topic, _options.ConsumerGroup, _options.ConsumerName,
                    UndeliveredMessages, _options.ReadBatchSize).ConfigureAwait(false);
                var handledNew = await HandleEntriesAsync(db, topic, entries, onMessage, cancellationToken).ConfigureAwait(false);

                if (!handledClaimed && !handledNew)
                    await Task.Delay(_options.PollInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error reading Redis stream {Stream}", topic);
                await Task.Delay(_options.PollInterval, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<bool> HandleEntriesAsync(
        IDatabaseAsync db,
        string topic,
        StreamEntry[] entries,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken)
    {
        if (entries is null || entries.Length == 0)
            return false;

        foreach (var entry in entries)
        {
            if (cancellationToken.IsCancellationRequested)
                break;

            var payload = entry[DataField];
            if (payload.IsNull)
            {
                // No usable payload; drop it so it does not linger in the pending list.
                await db.StreamAcknowledgeAsync(topic, _options.ConsumerGroup, entry.Id).ConfigureAwait(false);
                continue;
            }

            MessageDisposition disposition;
            try
            {
                disposition = await onMessage(payload.ToString(), topic).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error dispatching Redis message from {Stream}; leaving pending.", topic);
                disposition = MessageDisposition.Requeue;
            }

            // Acknowledge and DeadLetter remove the entry from the pending list (Redis has no native DLQ).
            // Requeue leaves it pending to be reclaimed after ClaimIdleTime and redelivered.
            if (disposition != MessageDisposition.Requeue)
                await db.StreamAcknowledgeAsync(topic, _options.ConsumerGroup, entry.Id).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Verifies reachability with a PING against the server.
    /// </summary>
    internal async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        var db = await GetDatabaseAsync().ConfigureAwait(false);
        await db.ExecuteAsync("PING").ConfigureAwait(false);
        return true;
    }

    private async Task<IDatabase> GetDatabaseAsync()
    {
        var multiplexer = await _connection.Value.ConfigureAwait(false);
        return multiplexer.GetDatabase();
    }

    public void Dispose()
    {
        if (_connection.IsValueCreated && _connection.Value is { IsCompletedSuccessfully: true } task)
            task.Result.Dispose();
    }
}
