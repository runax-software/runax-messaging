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

    private static readonly Dictionary<string, string> ProbeHeaders = new()
    {
        ["tenant"] = "acme",
        [MessageHeaders.TraceParent] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    };

    [Theory]
    [MemberData(nameof(Engines))]
    public async Task Transport_round_trips_the_envelope_and_native_headers(string configuration)
    {
        await using var connection = await ConnectionMultiplexer.ConnectAsync(configuration);
        var topic = $"runax-test-{Guid.NewGuid():N}";

        try
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new RedisConfig
            {
                Configuration = configuration,
                PollInterval = TimeSpan.FromMilliseconds(100),
            })));
            await using var provider = services.BuildServiceProvider();
            var transport = provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);

            var received = new TaskCompletionSource<TransportMessage>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

            var subscription = transport.SubscribeAsync([topic], message =>
            {
                received.TrySetResult(message);
                return ValueTask.FromResult(MessageDisposition.Acknowledge);
            }, cts.Token);

            // Let the consumer group get created and the pump start before publishing.
            await Task.Delay(500);

            var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
            await transport.PublishAsync(new TransportMessage(topic, envelope, ProbeHeaders));

            var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(15));
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
        // The transport config auto-registers its health check as "runax:default".
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new RedisConfig { Configuration = configuration })));
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(r => r.Name == $"runax:{BusNames.Default}");

        report.Status.ShouldBe(HealthStatus.Healthy);
        report.Entries[$"runax:{BusNames.Default}"].Status.ShouldBe(HealthStatus.Healthy);
    }
}
