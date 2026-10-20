using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Kafka;

namespace Runax.Messaging.Transports.Kafka.Tests;

/// <summary>
/// Integration tests for the Kafka transport.
/// Requires Kafka on localhost:9092 (or KAFKA_BOOTSTRAP) — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class KafkaTransportIntegrationTests : IDisposable
{
    private static string BootstrapServers =>
        Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";

    private readonly string _topic = $"runax.test.{Guid.NewGuid():N}";

    private readonly ServiceProvider _provider;

    public KafkaTransportIntegrationTests()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new KafkaConfig
        {
            BootstrapServers = BootstrapServers,
            ConsumerGroupId = $"runax-test-{Guid.NewGuid():N}",
        })));
        _provider = services.BuildServiceProvider();
    }

    public void Dispose() => _provider.Dispose();

    private static readonly Dictionary<string, string> ProbeHeaders = new()
    {
        ["tenant"] = "acme",
        [MessageHeaders.TraceParent] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    };

    [Fact]
    public async Task Publish_then_subscribe_round_trips_the_envelope_and_native_headers()
    {
        var transport = _provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);
        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var received = new TaskCompletionSource<TransportMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = transport.SubscribeAsync(
            [_topic],
            message =>
            {
                received.TrySetResult(message);
                return ValueTask.FromResult(MessageDisposition.Acknowledge);
            },
            cts.Token);

        // Give the consumer time to join the group and get its partition assignment.
        await Task.Delay(3000);
        await transport.PublishAsync(new TransportMessage(_topic, envelope, ProbeHeaders));

        var result = await received.Task.WaitAsync(cts.Token);
        result.Body.ShouldBe(envelope);
        result.Headers.ShouldContainKeyAndValue("tenant", "acme");
        result.Headers.ShouldContainKeyAndValue(MessageHeaders.TraceParent, ProbeHeaders[MessageHeaders.TraceParent]);

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

    [Fact]
    public async Task Batch_publish_delivers_every_message()
    {
        var transport = _provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);
        const int count = 20;

        await transport.PublishBatchAsync(_topic,
            Enumerable.Range(0, count).Select(i => $$"""{"n":{{i}}}""").ToList());

        using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"runax-test-drain-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();
        consumer.Subscribe(_topic);

        var received = 0;
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (received < count && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result?.Message is not null)
                received++;
        }

        consumer.Close();
        received.ShouldBe(count);
    }
}
