using System.Text;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Runax.Messaging.Abstractions;
using Runax.Messaging.RabbitMq;

namespace Runax.Messaging.RabbitMq.Tests;

/// <summary>
/// Integration tests for the RabbitMQ transport.
/// Requires RabbitMQ on localhost:5672 (or RABBITMQ_HOST) — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RabbitMqTransportIntegrationTests : IAsyncLifetime, IDisposable
{
    private static string HostName => Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";

    private readonly string _exchange = $"runax.test.{Guid.NewGuid():N}";
    private readonly string _topic = "orders.placed";

    private ServiceProvider _provider = null!;
    private IConnection _connection = null!;
    private IModel _channel = null!;
    private string _queue = null!;

    public ValueTask InitializeAsync()
    {
        var factory = new ConnectionFactory { HostName = HostName, DispatchConsumersAsync = true };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();

        // Bind a queue before publishing — a topic exchange drops messages with no matching binding.
        _channel.ExchangeDeclare(_exchange, ExchangeType.Topic, durable: true);
        _queue = _channel.QueueDeclare(queue: string.Empty, durable: false, exclusive: true, autoDelete: true).QueueName;
        _channel.QueueBind(_queue, _exchange, _topic);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddRabbitMq(o =>
        {
            o.HostName = HostName;
            o.ExchangeName = _exchange;
        }));
        _provider = services.BuildServiceProvider();

        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public void Dispose()
    {
        _provider.Dispose();
        _channel.Dispose();
        _connection.Dispose();
    }

    [Fact]
    public async Task Publish_routes_the_envelope_to_a_bound_queue()
    {
        var transport = _provider.GetRequiredService<IMessagingTransport>();
        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";

        await transport.PublishAsync(_topic, envelope);

        BasicGetResult? result = null;
        for (var attempt = 0; attempt < 20 && result is null; attempt++)
        {
            result = _channel.BasicGet(_queue, autoAck: true);
            if (result is null)
                await Task.Delay(100);
        }

        result.ShouldNotBeNull();
        Encoding.UTF8.GetString(result!.Body.ToArray()).ShouldBe(envelope);
    }

    [Fact]
    public async Task Publish_marks_messages_persistent_and_json()
    {
        var transport = _provider.GetRequiredService<IMessagingTransport>();

        await transport.PublishAsync(_topic, """{"x":1}""");

        BasicGetResult? result = null;
        for (var attempt = 0; attempt < 20 && result is null; attempt++)
        {
            result = _channel.BasicGet(_queue, autoAck: true);
            if (result is null)
                await Task.Delay(100);
        }

        result.ShouldNotBeNull();
        result!.BasicProperties.ContentType.ShouldBe("application/json");
        result.BasicProperties.DeliveryMode.ShouldBe((byte)2);
    }
}
