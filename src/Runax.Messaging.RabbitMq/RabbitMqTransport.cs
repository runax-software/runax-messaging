using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.RabbitMq;

/// <summary>
/// RabbitMQ implementation of <see cref="IMessagingTransport"/>. Topics map to routing keys on a topic exchange.
/// </summary>
internal sealed class RabbitMqTransport : IMessagingTransport, IDisposable
{
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqTransport> _logger;
    private readonly Lazy<IConnection> _connection;
    private readonly PublishChannelPool _publishPool;
    private IModel? _subscribeChannel;

    public RabbitMqTransport(RabbitMqOptions options, ILogger<RabbitMqTransport> logger)
    {
        _options = options;
        _logger = logger;
        _connection = new Lazy<IConnection>(() =>
        {
            var factory = new ConnectionFactory
            {
                HostName = _options.HostName,
                Port = _options.Port,
                UserName = _options.UserName,
                Password = _options.Password,
                VirtualHost = _options.VirtualHost,
                DispatchConsumersAsync = true,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true
            };

            return factory.CreateConnection();
        });

        _publishPool = new PublishChannelPool(
            Math.Max(1, _options.PublishChannelPoolSize),
            () => CreateChannel(publisherConfirms: _options.PublisherConfirms));
    }

    public async ValueTask PublishAsync(
        string topic,
        string envelopeJson,
        CancellationToken cancellationToken = default)
    {
        var body = Encoding.UTF8.GetBytes(envelopeJson);

        // IModel is not thread-safe; each pooled channel is used by a single publisher at a time,
        // so concurrent publishes fan out across channels instead of serializing on one lock.
        var channel = await _publishPool.RentAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var properties = channel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.DeliveryMode = 2; // persistent

            channel.BasicPublish(
                exchange: _options.ExchangeName,
                routingKey: topic,
                basicProperties: properties,
                body: body);

            if (_options.PublisherConfirms)
                channel.WaitForConfirmsOrDie(_options.ConfirmTimeout);

            _publishPool.Return(channel);
        }
        catch
        {
            // A failed publish or confirm may leave unconfirmed state on the channel; drop it.
            _publishPool.Discard(channel);
            throw;
        }
    }

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        var channel = CreateChannel();
        _subscribeChannel = channel;
        channel.BasicQos(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false);

        var arguments = new Dictionary<string, object>();
        if (!string.IsNullOrEmpty(_options.DeadLetterExchange))
        {
            channel.ExchangeDeclare(_options.DeadLetterExchange, _options.DeadLetterExchangeType, durable: true);
            arguments["x-dead-letter-exchange"] = _options.DeadLetterExchange;
        }

        var queueName = channel.QueueDeclare(
            queue: string.Empty,
            durable: false,
            exclusive: true,
            autoDelete: true,
            arguments: arguments.Count > 0 ? arguments : null).QueueName;

        foreach (var topic in topics)
        {
            channel.QueueBind(queue: queueName, exchange: _options.ExchangeName, routingKey: topic);
            _logger.LogInformation("Bound queue {Queue} to topic {Topic}", queueName, topic);
        }

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.Received += async (_, ea) =>
        {
            MessageDisposition disposition;
            try
            {
                var json = Encoding.UTF8.GetString(ea.Body.Span);
                disposition = await onMessage(json, ea.RoutingKey);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error dispatching RabbitMQ message; requeueing.");
                disposition = MessageDisposition.Requeue;
            }

            switch (disposition)
            {
                case MessageDisposition.Acknowledge:
                    channel.BasicAck(ea.DeliveryTag, multiple: false);
                    break;
                case MessageDisposition.DeadLetter:
                    // Reject without requeue: routes to the queue's dead-letter exchange when configured,
                    // otherwise the broker discards the message.
                    channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
                    break;
                case MessageDisposition.Requeue:
                default:
                    channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: true);
                    break;
            }
        };

        channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

        _logger.LogInformation("RabbitMQ consumer started on queue {Queue} for {Count} topic(s)",
            queueName, topics.Length);

        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("RabbitMQ consumer shutting down");
        }
    }

    private IModel CreateChannel(bool publisherConfirms = false)
    {
        var channel = _connection.Value.CreateModel();
        channel.ExchangeDeclare(_options.ExchangeName, _options.ExchangeType, durable: true);

        if (publisherConfirms) channel.ConfirmSelect();

        return channel;
    }

    public void Dispose()
    {
        _subscribeChannel?.Dispose();
        _publishPool.Dispose();

        if (_connection.IsValueCreated) _connection.Value.Dispose();
    }

    /// <summary>
    /// Fixed-size pool of publish channels. A rented channel is owned exclusively by one caller until
    /// returned, which keeps the non-thread-safe <see cref="IModel"/> confined to a single thread at a time.
    /// </summary>
    private sealed class PublishChannelPool(int size, Func<IModel> channelFactory) : IDisposable
    {
        private readonly SemaphoreSlim _gate = new(size, size);
        private readonly ConcurrentBag<IModel> _channels = new();

        public async ValueTask<IModel> RentAsync(CancellationToken cancellationToken)
        {
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (_channels.TryTake(out var channel) && channel.IsOpen) return channel;

            channel?.Dispose();
            return channelFactory();
        }

        public void Return(IModel channel)
        {
            if (channel.IsOpen) _channels.Add(channel);
            else channel.Dispose();
            _gate.Release();
        }

        public void Discard(IModel channel)
        {
            channel.Dispose();
            _gate.Release();
        }

        public void Dispose()
        {
            while (_channels.TryTake(out var channel)) channel.Dispose();

            _gate.Dispose();
        }
    }
}
