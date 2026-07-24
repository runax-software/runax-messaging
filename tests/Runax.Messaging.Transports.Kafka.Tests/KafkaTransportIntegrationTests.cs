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
        services.AddRunaxMessaging(m => m.AddKafka(kafka => kafka.Configure(o =>
        {
            o.BootstrapServers = BootstrapServers;
            o.ConsumerGroupId = $"runax-test-{Guid.NewGuid():N}";
        })));
        _provider = services.BuildServiceProvider();
    }

    public void Dispose() => _provider.Dispose();

    [Fact]
    public async Task Publish_then_subscribe_round_trips_the_envelope()
    {
        var transport = _provider.GetRequiredService<IMessagingTransport>();
        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = transport.SubscribeAsync(
            [_topic],
            (json, _) =>
            {
                received.TrySetResult(json);
                return ValueTask.FromResult(MessageDisposition.Acknowledge);
            },
            cts.Token);

        // Give the consumer time to join the group and get its partition assignment.
        await Task.Delay(3000);
        await transport.PublishAsync(_topic, envelope);

        var delivered = await received.Task.WaitAsync(cts.Token);
        delivered.ShouldBe(envelope);

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
        var transport = _provider.GetRequiredService<IMessagingTransport>();
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
