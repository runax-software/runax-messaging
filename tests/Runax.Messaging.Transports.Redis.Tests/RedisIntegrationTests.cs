using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Redis;
using StackExchange.Redis;

namespace Runax.Messaging.Transports.Redis.Tests;

/// <summary>
/// Integration tests for the Redis Streams transport, run against both engines.
/// Requires Valkey on localhost:6379 and Redis on localhost:6380 — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RedisIntegrationTests
{
    // Same transport, exercised against both a Valkey and a Redis server.
    public static TheoryData<string> Engines =>
    [
        Environment.GetEnvironmentVariable("VALKEY_HOST") ?? "localhost:6379",
        Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost:6380",
    ];

    [Theory]
    [MemberData(nameof(Engines))]
    public async Task Transport_round_trips_publish_to_subscribe(string configuration)
    {
        await using var connection = await ConnectionMultiplexer.ConnectAsync(configuration);
        var topic = $"runax-test-{Guid.NewGuid():N}";

        try
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddRunaxMessaging(m => m.AddRedis(__tb => __tb.Configure(o =>
            {
                o.Configuration = configuration;
                o.PollInterval = TimeSpan.FromMilliseconds(100);
            })));
            await using var provider = services.BuildServiceProvider();
            var transport = provider.GetRequiredService<IMessagingTransport>();

            var received = new TaskCompletionSource<string>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

            var subscription = transport.SubscribeAsync([topic], (json, _) =>
            {
                received.TrySetResult(json);
                return ValueTask.FromResult(MessageDisposition.Acknowledge);
            }, cts.Token);

            // Let the consumer group get created and the pump start before publishing.
            await Task.Delay(500);

            var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
            await transport.PublishAsync(topic, envelope);

            var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(15));
            result.ShouldBe(envelope);

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
        finally
        {
            await connection.GetDatabase().KeyDeleteAsync(topic);
        }
    }

    [Theory]
    [MemberData(nameof(Engines))]
    public async Task Health_check_reports_healthy(string configuration)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddRedis(__tb => __tb.Configure(o => o.Configuration = configuration)));
        services.AddHealthChecks().AddRedisTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Healthy);
    }
}
