using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.RabbitMq;

/// <summary>
/// RabbitMQ implementation of <see cref="IMessagingTransport"/>. Topics map to routing keys on a topic exchange,
/// and native headers travel in the AMQP headers table. Built against the RabbitMQ.Client 7.x asynchronous
/// <see cref="IChannel"/> API.
/// </summary>
internal sealed class RabbitMqTransport : IMessagingTransport, IDisposable
{
    private readonly RabbitMqConfig _options;
    private readonly ILogger<RabbitMqTransport> _logger;
    private readonly Lazy<Task<IConnection>> _connection;
    private readonly PublishChannelPool _publishPool;
    private IChannel? _subscribeChannel;

    public RabbitMqTransport(RabbitMqConfig options, ILogger<RabbitMqTransport> logger)
    {
        _options = options;
        _logger = logger;
        _connection = new Lazy<Task<IConnection>>(CreateConnectionAsync);

        _publishPool = new PublishChannelPool(
            Math.Max(1, _options.PublishChannelPoolSize),
            cancellationToken => CreateChannelAsync(_options.PublisherConfirms, cancellationToken));
    }

    internal const string TransportName = "rabbitmq";

    public string SystemName => TransportName;

    public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default) =>
        PublishAsync(new TransportMessage(topic, envelopeJson), cancellationToken);

    public async ValueTask PublishAsync(TransportMessage message, CancellationToken cancellationToken = default)
    {
        var body = Encoding.UTF8.GetBytes(message.Body);

        // Each pooled channel is used by a single publisher at a time, so concurrent publishes
        // fan out across channels. With confirmations enabled, BasicPublishAsync awaits the broker ack.
        var channel = await _publishPool.RentAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var confirm = ConfirmScope(cancellationToken);
            await channel.BasicPublishAsync(
                _options.ExchangeName, message.Topic, mandatory: false, CreateProperties(message.Headers), body,
                confirm.Token)
                .ConfigureAwait(false);

            _publishPool.Return(channel);
        }
        catch
        {
            // A failed publish or confirm may leave the channel in an unclean state; drop it.
            _publishPool.Discard(channel);
            throw;
        }
    }

    public ValueTask PublishBatchAsync(
        string topic,
        IReadOnlyList<string> envelopeJsons,
        CancellationToken cancellationToken = default) =>
        PublishBatchAsync(envelopeJsons.Select(e => new TransportMessage(topic, e)).ToList(), cancellationToken);

    public async ValueTask PublishBatchAsync(
        IReadOnlyList<TransportMessage> messages,
        CancellationToken cancellationToken = default)
    {
        if (messages.Count == 0)
            return;

        var channel = await _publishPool.RentAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            using var confirm = ConfirmScope(cancellationToken);

            // Pipeline every publish, then await all confirmations for the batch together.
            var pending = new List<Task>(messages.Count);
            foreach (var message in messages)
            {
                pending.Add(channel.BasicPublishAsync(
                    _options.ExchangeName, message.Topic, mandatory: false, CreateProperties(message.Headers),
                    Encoding.UTF8.GetBytes(message.Body), confirm.Token).AsTask());
            }

            await Task.WhenAll(pending).ConfigureAwait(false);

            _publishPool.Return(channel);
        }
        catch
        {
            _publishPool.Discard(channel);
            throw;
        }
    }

    public Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default) =>
        SubscribeAsync(topics, message => onMessage(message.Body, message.Topic), cancellationToken);

    public async Task SubscribeAsync(
        string[] topics,
        Func<TransportMessage, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        var channel = await CreateChannelAsync(publisherConfirms: false, cancellationToken).ConfigureAwait(false);
        _subscribeChannel = channel;
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false, cancellationToken)
            .ConfigureAwait(false);

        var arguments = new Dictionary<string, object?>();
        if (!string.IsNullOrEmpty(_options.DeadLetterExchange))
        {
            await channel.ExchangeDeclareAsync(
                _options.DeadLetterExchange, _options.DeadLetterExchangeType,
                durable: true, autoDelete: false, arguments: null, passive: false, noWait: false, cancellationToken)
                .ConfigureAwait(false);
            arguments["x-dead-letter-exchange"] = _options.DeadLetterExchange;
        }

        var declareOk = await channel.QueueDeclareAsync(
            queue: string.Empty, durable: false, exclusive: true, autoDelete: true,
            arguments: arguments.Count > 0 ? arguments : null, passive: false, noWait: false, cancellationToken)
            .ConfigureAwait(false);
        var queueName = declareOk.QueueName;

        foreach (var topic in topics)
        {
            await channel.QueueBindAsync(queueName, _options.ExchangeName, topic, arguments: null, noWait: false, cancellationToken)
                .ConfigureAwait(false);
            _logger.LogInformation("Bound queue {Queue} to topic {Topic}", queueName, topic);
        }

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, ea) => OnReceivedAsync(channel, onMessage, ea);

        await channel.BasicConsumeAsync(queueName, autoAck: false, consumer, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("RabbitMQ consumer started on queue {Queue} for {Count} topic(s)",
            queueName, topics.Length);

        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("RabbitMQ consumer shutting down");
        }
    }

    private async Task OnReceivedAsync(
        IChannel channel,
        Func<TransportMessage, ValueTask<MessageDisposition>> onMessage,
        BasicDeliverEventArgs ea)
    {
        MessageDisposition disposition;
        try
        {
            var json = Encoding.UTF8.GetString(ea.Body.Span);
            disposition = await onMessage(
                new TransportMessage(ea.RoutingKey, json, ReadHeaders(ea.BasicProperties.Headers)))
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error dispatching RabbitMQ message; requeueing.");
            disposition = MessageDisposition.Requeue;
        }

        // Settle with a fresh token so the verdict is applied even during graceful shutdown.
        try
        {
            switch (disposition)
            {
                case MessageDisposition.Acknowledge:
                    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, CancellationToken.None).ConfigureAwait(false);
                    break;
                case MessageDisposition.DeadLetter:
                    // Reject without requeue: routes to the queue's dead-letter exchange when configured,
                    // otherwise the broker discards the message.
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, CancellationToken.None).ConfigureAwait(false);
                    break;
                case MessageDisposition.Requeue:
                default:
                    await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true, CancellationToken.None).ConfigureAwait(false);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to settle RabbitMQ message on delivery tag {DeliveryTag}.", ea.DeliveryTag);
        }
    }

    private Task<IConnection> CreateConnectionAsync()
    {
        var factory = new ConnectionFactory
        {
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true
        };

        if (!string.IsNullOrEmpty(_options.Uri))
        {
            // A full amqp(s):// URI takes precedence and carries its own TLS + credentials.
            factory.Uri = new Uri(_options.Uri);
        }
        else
        {
            factory.HostName = _options.HostName;
            factory.Port = _options.Port;
            factory.UserName = _options.UserName;
            factory.Password = _options.Password;
            factory.VirtualHost = _options.VirtualHost;

            if (_options.UseTls)
            {
                factory.Ssl = new SslOption
                {
                    Enabled = true,
                    ServerName = _options.SslServerName ?? _options.HostName
                };
            }
        }

        return factory.CreateConnectionAsync();
    }

    private async ValueTask<IChannel> CreateChannelAsync(bool publisherConfirms, CancellationToken cancellationToken)
    {
        var connection = await _connection.Value.WaitAsync(cancellationToken).ConfigureAwait(false);

        var options = new CreateChannelOptions(
            publisherConfirmationsEnabled: publisherConfirms,
            publisherConfirmationTrackingEnabled: publisherConfirms);

        var channel = await connection.CreateChannelAsync(options, cancellationToken).ConfigureAwait(false);
        await channel.ExchangeDeclareAsync(
            _options.ExchangeName, _options.ExchangeType,
            durable: true, autoDelete: false, arguments: null, passive: false, noWait: false, cancellationToken)
            .ConfigureAwait(false);

        return channel;
    }

    internal static BasicProperties CreateProperties(IReadOnlyDictionary<string, string> headers) => new()
    {
        ContentType = "application/json",
        DeliveryMode = DeliveryModes.Persistent,
        Headers = headers.Count == 0 ? null : headers.ToDictionary(h => h.Key, h => (object?)h.Value),
    };

    // String headers arrive as AMQP long strings (raw bytes); other AMQP types are formatted.
    internal static Dictionary<string, string>? ReadHeaders(IDictionary<string, object?>? headers)
    {
        if (headers is null || headers.Count == 0)
            return null;

        var result = new Dictionary<string, string>(headers.Count, StringComparer.Ordinal);
        foreach (var (key, value) in headers)
        {
            if (MessageHeaders.FormatValue(value) is { } text)
                result[key] = text;
        }

        return result;
    }

    // Bounds the confirm wait to ConfirmTimeout when publisher confirmations are enabled.
    private ConfirmTokenScope ConfirmScope(CancellationToken cancellationToken)
    {
        if (!_options.PublisherConfirms)
            return new ConfirmTokenScope(null, cancellationToken);

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_options.ConfirmTimeout);
        return new ConfirmTokenScope(cts, cts.Token);
    }

    /// <summary>
    /// Verifies broker reachability by opening (lazily) the shared connection and checking it is live.
    /// </summary>
    internal async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        var connection = await _connection.Value.WaitAsync(cancellationToken).ConfigureAwait(false);
        return connection.IsOpen;
    }

    public void Dispose()
    {
        _subscribeChannel?.Dispose();
        _publishPool.Dispose();

        if (_connection.IsValueCreated && _connection.Value is { IsCompletedSuccessfully: true } task)
            task.Result.Dispose();
    }

    private readonly struct ConfirmTokenScope(CancellationTokenSource? source, CancellationToken token) : IDisposable
    {
        public CancellationToken Token { get; } = token;

        public void Dispose() => source?.Dispose();
    }

    /// <summary>
    /// Fixed-size pool of publish channels. A rented channel is owned exclusively by one caller until
    /// returned, which keeps the non-thread-safe <see cref="IChannel"/> confined to a single caller at a time.
    /// </summary>
    private sealed class PublishChannelPool(int size, Func<CancellationToken, ValueTask<IChannel>> channelFactory) : IDisposable
    {
        private readonly SemaphoreSlim _gate = new(size, size);
        private readonly ConcurrentBag<IChannel> _channels = new();

        public async ValueTask<IChannel> RentAsync(CancellationToken cancellationToken)
        {
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_channels.TryTake(out var channel) && channel.IsOpen)
                    return channel;

                channel?.Dispose();
                return await channelFactory(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                _gate.Release();
                throw;
            }
        }

        public void Return(IChannel channel)
        {
            if (channel.IsOpen)
                _channels.Add(channel);
            else
                channel.Dispose();

            _gate.Release();
        }

        public void Discard(IChannel channel)
        {
            channel.Dispose();
            _gate.Release();
        }

        public void Dispose()
        {
            while (_channels.TryTake(out var channel))
                channel.Dispose();

            _gate.Dispose();
        }
    }
}
