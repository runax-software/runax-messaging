using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Azure.ServiceBus;

namespace Runax.Messaging.Transports.Azure.ServiceBus.Tests;

/// <summary>
/// Integration tests for the Azure Service Bus transport.
/// Requires the Service Bus emulator on localhost:5673 (or SERVICEBUS_CONNECTION_STRING) — see compose.yml.
/// The topic and subscription are pre-declared in docker/servicebus-config.json.
/// </summary>
[Trait("Category", "Integration")]
public sealed class AzureServiceBusIntegrationTests
{
    private const string Topic = "runax.test.topic";
    private const string Subscription = "worker";

    private static string ConnectionString =>
        Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTION_STRING")
        ?? "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    private static readonly Dictionary<string, string> ProbeHeaders = new()
    {
        ["tenant"] = "acme",
        [MessageHeaders.TraceParent] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    };

    [Fact]
    public async Task Transport_round_trips_the_envelope_and_native_headers()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new AzureServiceBusConfig
        {
            ConnectionString = ConnectionString,
            TopicSubscriptionMap = { [Topic] = Subscription },
        })));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);

        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
        var received = new TaskCompletionSource<TransportMessage>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(40));

        // The topic/subscription is shared across runs; acknowledge everything and match our own probe.
        var subscription = transport.SubscribeAsync([Topic], message =>
        {
            if (message.Body == envelope)
                received.TrySetResult(message);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        await Task.Delay(500);
        await transport.PublishAsync(new TransportMessage(Topic, envelope, ProbeHeaders));

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(35));
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
}
