using System.Text;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Runax.Messaging.Abstractions;
using Runax.Messaging.RabbitMq;

namespace Runax.Messaging.RabbitMq.Tests;

/// <summary>
/// Integration tests for broker-native dead-lettering (DeadLetter disposition → dead-letter exchange).
/// Requires RabbitMQ on localhost:5672 (or RABBITMQ_HOST) — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RabbitMqDeadLetterIntegrationTests : IAsyncLifetime, IDisposable
{
    private static string HostName => Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";

    private readonly string _exchange = $"runax.test.{Guid.NewGuid():N}";
    private readonly string _deadLetterExchange = $"runax.test.dlx.{Guid.NewGuid():N}";
    private readonly string _topic = "orders.rejected";

    private ServiceProvider _provider = null!;
    private IConnection _connection = null!;
    private IModel _channel = null!;
    private string _deadLetterQueue = null!;

    public ValueTask InitializeAsync()
    {
        var factory = new ConnectionFactory { HostName = HostName, DispatchConsumersAsync = true };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();

        // Stand up the dead-letter exchange and a queue bound to it so rejected messages have somewhere to land.
        _channel.ExchangeDeclare(_exchange, ExchangeType.Topic, durable: true);
        _channel.ExchangeDeclare(_deadLetterExchange, ExchangeType.Topic, durable: true);
        _deadLetterQueue = _channel.QueueDeclare(queue: string.Empty, durable: false, exclusive: true, autoDelete: true).QueueName;
        _channel.QueueBind(_deadLetterQueue, _deadLetterExchange, _topic);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddRabbitMq(o =>
        {
            o.HostName = HostName;
            o.ExchangeName = _exchange;
            o.DeadLetterExchange = _deadLetterExchange;
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
    public async Task Rejected_message_is_routed_to_the_dead_letter_exchange()
    {
        var transport = _provider.GetRequiredService<IMessagingTransport>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var subscription = transport.SubscribeAsync(
            [_topic],
            (_, _) => ValueTask.FromResult(MessageDisposition.DeadLetter),
            cts.Token);

        // Give the subscriber time to declare and bind its queue before publishing on the topic exchange.
        await Task.Delay(500);

        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
        await transport.PublishAsync(_topic, envelope);

        BasicGetResult? result = null;
        for (var attempt = 0; attempt < 40 && result is null; attempt++)
        {
            result = _channel.BasicGet(_deadLetterQueue, autoAck: true);
            if (result is null)
                await Task.Delay(100);
        }

        result.ShouldNotBeNull();
        Encoding.UTF8.GetString(result!.Body.ToArray()).ShouldBe(envelope);

        await cts.CancelAsync();
        try
        {
            await subscription;
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }
}
