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
    private readonly Lock _publishLock = new();
    private IModel? _publishChannel;
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
    }

    public ValueTask PublishAsync(
        string topic,
        string envelopeJson,
        CancellationToken cancellationToken = default)
    {
        var body = Encoding.UTF8.GetBytes(envelopeJson);

        // IModel is not thread-safe; serialize publishes (and their confirms) on the shared channel.
        lock (_publishLock)
        {
            _publishChannel ??= CreateChannel(publisherConfirms: _options.PublisherConfirms);

            var properties = _publishChannel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.DeliveryMode = 2; // persistent

            _publishChannel.BasicPublish(
                exchange: _options.ExchangeName,
                routingKey: topic,
                basicProperties: properties,
                body: body);

            if (_options.PublisherConfirms)
                _publishChannel.WaitForConfirmsOrDie(_options.ConfirmTimeout);
        }

        return ValueTask.CompletedTask;
    }

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        var channel = CreateChannel();
        _subscribeChannel = channel;
        channel.BasicQos(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false);

        var queueName = channel.QueueDeclare(
            queue: string.Empty,
            durable: false,
            exclusive: true,
            autoDelete: true).QueueName;

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

            if (disposition == MessageDisposition.Acknowledge)
                channel.BasicAck(ea.DeliveryTag, multiple: false);
            else
                channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: true);
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

        if (publisherConfirms)
            channel.ConfirmSelect();

        return channel;
    }

    public void Dispose()
    {
        _subscribeChannel?.Dispose();
        _publishChannel?.Dispose();

        if (_connection.IsValueCreated)
            _connection.Value.Dispose();
    }
}
