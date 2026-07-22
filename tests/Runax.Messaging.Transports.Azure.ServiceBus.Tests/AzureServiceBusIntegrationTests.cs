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

    [Fact]
    public async Task Transport_round_trips_publish_to_subscribe()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddAzureServiceBus(o =>
        {
            o.ConnectionString = ConnectionString;
            o.TopicSubscriptionMap[Topic] = Subscription;
        }));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IMessagingTransport>();

        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
        var received = new TaskCompletionSource<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(40));

        // The topic/subscription is shared across runs; acknowledge everything and match our own probe.
        var subscription = transport.SubscribeAsync([Topic], (json, _) =>
        {
            if (json == envelope)
                received.TrySetResult(json);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        await Task.Delay(500);
        await transport.PublishAsync(Topic, envelope);

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(35));
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
}
