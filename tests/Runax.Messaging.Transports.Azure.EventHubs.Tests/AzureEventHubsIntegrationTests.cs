using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Azure.EventHubs;

namespace Runax.Messaging.Transports.Azure.EventHubs.Tests;

/// <summary>
/// Integration tests for the Azure Event Hubs transport.
/// Requires a live Event Hubs namespace (EVENTHUBS_CONNECTION_STRING) plus an Azure Storage account for
/// the blob checkpoint store (EVENTHUBS_BLOB_CONNECTION_STRING). The event hub named <see cref="Topic"/>,
/// its consumer group, and the blob container must be provisioned ahead of time.
/// </summary>
[Trait("Category", "Integration")]
public sealed class AzureEventHubsIntegrationTests
{
    private const string Topic = "runax.test.topic";
    private const string BlobContainer = "runax-checkpoints";

    private static string ConnectionString =>
        Environment.GetEnvironmentVariable("EVENTHUBS_CONNECTION_STRING")
        ?? throw new InvalidOperationException("EVENTHUBS_CONNECTION_STRING is not set.");

    private static string BlobConnectionString =>
        Environment.GetEnvironmentVariable("EVENTHUBS_BLOB_CONNECTION_STRING")
        ?? throw new InvalidOperationException("EVENTHUBS_BLOB_CONNECTION_STRING is not set.");

    [Fact]
    public async Task Transport_round_trips_publish_to_subscribe()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddAzureEventHubs(eventHubs => eventHubs.Configure(o =>
        {
            o.ConnectionString = ConnectionString;
            o.BlobConnectionString = BlobConnectionString;
            o.BlobContainerName = BlobContainer;
        })));
        await using var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<IMessagingTransport>();

        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
        var received = new TaskCompletionSource<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        // The hub is shared across runs; acknowledge everything and match our own probe.
        var subscription = transport.SubscribeAsync([Topic], (json, _) =>
        {
            if (json == envelope)
                received.TrySetResult(json);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        await Task.Delay(TimeSpan.FromSeconds(5));
        await transport.PublishAsync(Topic, envelope);

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(50));
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
