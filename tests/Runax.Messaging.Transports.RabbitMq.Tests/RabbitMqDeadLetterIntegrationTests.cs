using System.Text;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.RabbitMq;

namespace Runax.Messaging.Transports.RabbitMq.Tests;

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
    private IChannel _channel = null!;
    private string _deadLetterQueue = null!;

    public async ValueTask InitializeAsync()
    {
        var factory = new ConnectionFactory { HostName = HostName };
        _connection = await factory.CreateConnectionAsync();
        _channel = await _connection.CreateChannelAsync();

        // Stand up the dead-letter exchange and a queue bound to it so rejected messages have somewhere to land.
        await _channel.ExchangeDeclareAsync(_exchange, ExchangeType.Topic,
            durable: true, autoDelete: false, arguments: null, passive: false, noWait: false, CancellationToken.None);
        await _channel.ExchangeDeclareAsync(_deadLetterExchange, ExchangeType.Topic,
            durable: true, autoDelete: false, arguments: null, passive: false, noWait: false, CancellationToken.None);
        var declareOk = await _channel.QueueDeclareAsync(queue: string.Empty,
            durable: false, exclusive: true, autoDelete: true, arguments: null, passive: false, noWait: false, CancellationToken.None);
        _deadLetterQueue = declareOk.QueueName;
        await _channel.QueueBindAsync(_deadLetterQueue, _deadLetterExchange, _topic, arguments: null, noWait: false, CancellationToken.None);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddRabbitMq(o =>
        {
            o.HostName = HostName;
            o.ExchangeName = _exchange;
            o.DeadLetterExchange = _deadLetterExchange;
        }));
        _provider = services.BuildServiceProvider();
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
            result = await _channel.BasicGetAsync(_deadLetterQueue, autoAck: true, CancellationToken.None);
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
