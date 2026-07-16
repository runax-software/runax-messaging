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
                DispatchConsumersAsync = true
            };

            return factory.CreateConnection();
        });
    }

    public ValueTask PublishAsync(
        string topic,
        string envelopeJson,
        CancellationToken cancellationToken = default)
    {
        _publishChannel ??= CreateChannel();

        var body = Encoding.UTF8.GetBytes(envelopeJson);
        var properties = _publishChannel.CreateBasicProperties();

        properties.ContentType = "application/json";
        properties.DeliveryMode = 2; // persistent

        _publishChannel.BasicPublish(
            exchange: _options.ExchangeName,
            routingKey: topic,
            basicProperties: properties,
            body: body);

        return ValueTask.CompletedTask;
    }

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask> onMessage,
        CancellationToken cancellationToken = default)
    {
        _subscribeChannel = CreateChannel();

        var queueName = _subscribeChannel.QueueDeclare(
            queue: string.Empty,
            durable: false,
            exclusive: true,
            autoDelete: true).QueueName;

        foreach (var topic in topics)
        {
            _subscribeChannel.QueueBind(queue: queueName, exchange: _options.ExchangeName, routingKey: topic);
            _logger.LogInformation("Bound queue {Queue} to topic {Topic}", queueName, topic);
        }

        var consumer = new AsyncEventingBasicConsumer(_subscribeChannel);

        consumer.Received += async (_, ea) =>
        {
            try
            {
                var json = Encoding.UTF8.GetString(ea.Body.Span);

                await onMessage(json, ea.RoutingKey);

                _subscribeChannel.BasicAck(ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message from RabbitMQ");
                _subscribeChannel.BasicNack(ea.DeliveryTag, multiple: false, requeue: true);
            }
        };

        _subscribeChannel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

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

    private IModel CreateChannel()
    {
        var channel = _connection.Value.CreateModel();
        channel.ExchangeDeclare(_options.ExchangeName, _options.ExchangeType, durable: true);

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
