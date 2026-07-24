using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Kafka;

namespace Runax.Messaging.Transports.Kafka.Tests;

/// <summary>
/// Integration tests for dead-lettering (DeadLetter disposition → {topic}{DeadLetterTopicSuffix}).
/// Requires Kafka on localhost:9092 (or KAFKA_BOOTSTRAP) — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class KafkaDeadLetterIntegrationTests : IDisposable
{
    private static string BootstrapServers =>
        Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";

    private readonly string _topic = $"runax.test.{Guid.NewGuid():N}";
    private const string DeadLetterSuffix = ".dead-letter";

    private readonly ServiceProvider _provider;

    public KafkaDeadLetterIntegrationTests()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddKafka(kafka => kafka.Configure(o =>
        {
            o.BootstrapServers = BootstrapServers;
            o.ConsumerGroupId = $"runax-test-{Guid.NewGuid():N}";
            o.DeadLetterTopicSuffix = DeadLetterSuffix;
        })));
        _provider = services.BuildServiceProvider();
    }

    public void Dispose() => _provider.Dispose();

    [Fact]
    public async Task Dead_lettered_message_is_produced_to_the_dead_letter_topic()
    {
        var transport = _provider.GetRequiredService<IMessagingTransport>();
        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(40));

        var subscription = transport.SubscribeAsync(
            [_topic],
            (_, _) => ValueTask.FromResult(MessageDisposition.DeadLetter),
            cts.Token);

        // Give the consumer time to join the group before publishing.
        await Task.Delay(3000);
        await transport.PublishAsync(_topic, envelope);

        using var consumer = new ConsumerBuilder<Null, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"runax-test-dlq-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();
        consumer.Subscribe(_topic + DeadLetterSuffix);

        string? deadLettered = null;
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (deadLettered is null && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result?.Message is not null)
                deadLettered = result.Message.Value;
        }

        consumer.Close();
        deadLettered.ShouldBe(envelope);

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
