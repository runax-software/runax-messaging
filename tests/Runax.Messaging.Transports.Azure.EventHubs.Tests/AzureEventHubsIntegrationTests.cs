using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Azure.EventHubs;

namespace Runax.Messaging.Transports.Azure.EventHubs.Tests;

/// <summary>
/// Integration tests for the Azure Event Hubs transport.
/// Runs against the Event Hubs emulator (AMQP on localhost:5674) with Azurite as the blob checkpoint store
/// (localhost:10000) — see compose.yml. The event hub named <see cref="Topic"/> and its <c>$Default</c>
/// consumer group are pre-declared in docker/eventhubs-config.json; the blob container is created on demand.
/// Override with EVENTHUBS_CONNECTION_STRING / EVENTHUBS_BLOB_CONNECTION_STRING to target a live namespace.
/// </summary>
[Trait("Category", "Integration")]
public sealed class AzureEventHubsIntegrationTests
{
    private const string Topic = "runax.test.topic";
    private const string ConsumerGroup = "runax";
    private const string BlobContainer = "runax-checkpoints";

    private static string ConnectionString =>
        Environment.GetEnvironmentVariable("EVENTHUBS_CONNECTION_STRING")
        ?? "Endpoint=sb://localhost:5674;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    private static string BlobConnectionString =>
        Environment.GetEnvironmentVariable("EVENTHUBS_BLOB_CONNECTION_STRING")
        ?? "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    private static readonly Dictionary<string, string> ProbeHeaders = new()
    {
        ["tenant"] = "acme",
        [MessageHeaders.TraceParent] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    };

    [Fact]
    public async Task Transport_round_trips_the_envelope_and_native_headers()
    {
        var services = new ServiceCollection();
        // Capture the transport's logs so a timeout can surface the EventProcessorClient error (the MTP
        // runner discards Console/ILogger output, but an exception message is shown in CI).
        var logs = new CapturingLoggerProvider();
        services.AddLogging(b => { b.SetMinimumLevel(LogLevel.Debug); b.AddProvider(logs); });
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new AzureEventHubsConfig
        {
            ConnectionString = ConnectionString,
            ConsumerGroup = ConsumerGroup,
            BlobConnectionString = BlobConnectionString,
            BlobContainerName = BlobContainer,
        })));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);

        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
        var received = new TaskCompletionSource<TransportMessage>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));

        // The hub is shared across runs; acknowledge everything and match our own probe.
        var subscription = transport.SubscribeAsync([Topic], message =>
        {
            if (message.Body == envelope)
                received.TrySetResult(message);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        // The EventProcessorClient needs time to claim its partition and begin reading; an event published
        // before that can be missed, so keep republishing the probe until the processor delivers it.
        var publishLoop = Task.Run(async () =>
        {
            while (!received.Task.IsCompleted && !cts.Token.IsCancellationRequested)
            {
                try
                {
                    await transport.PublishAsync(new TransportMessage(Topic, envelope, ProbeHeaders), cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(3), cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }, cts.Token);

        // SubscribeAsync runs until cancellation; if it completes early it faulted during startup, so await it
        // to surface the real exception instead of masking it as a plain timeout.
        var timeout = Task.Delay(TimeSpan.FromSeconds(110));
        var completed = await Task.WhenAny(received.Task, subscription, publishLoop, timeout);
        if (completed == subscription)
            await subscription;
        if (completed == publishLoop)
            await publishLoop;
        if (!received.Task.IsCompletedSuccessfully)
        {
            throw new TimeoutException(
                "Event Hubs round-trip timed out. Transport logs:\n" + string.Join("\n", logs.Messages));
        }

        var result = await received.Task;
        result.Body.ShouldBe(envelope);
        result.Headers.ShouldContainKeyAndValue("tenant", "acme");
        result.Headers.ShouldContainKeyAndValue(MessageHeaders.TraceParent, ProbeHeaders[MessageHeaders.TraceParent]);

        await cts.CancelAsync();
        foreach (var task in new[] { subscription, publishLoop })
        {
            try
            {
                await task;
            }
            catch (OperationCanceledException)
            {
                // Expected on shutdown.
            }
        }
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public System.Collections.Concurrent.ConcurrentQueue<string> Messages { get; } = new();

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(categoryName, Messages);

        public void Dispose() { }

        private sealed class CapturingLogger(string category, System.Collections.Concurrent.ConcurrentQueue<string> sink)
            : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                var line = $"[{logLevel}] {category}: {formatter(state, exception)}";
                if (exception is not null)
                    line += " | " + exception;
                sink.Enqueue(line);
            }
        }
    }
}
