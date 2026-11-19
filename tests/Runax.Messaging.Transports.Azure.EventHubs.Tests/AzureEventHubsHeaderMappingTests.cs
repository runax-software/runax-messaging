using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.EventHubs.Tests;

public class AzureEventHubsHeaderMappingTests
{
    [Fact]
    public void Native_headers_are_written_as_event_properties()
    {
        var eventData = AzureEventHubsTransport.ToEventData(
            new TransportMessage("orders", "{}", new Dictionary<string, string> { ["tenant"] = "acme" }));

        eventData.EventBody.ToString().ShouldBe("{}");
        eventData.Properties["tenant"].ShouldBe("acme");
    }

    [Fact]
    public void Received_event_properties_are_read_as_strings()
    {
        var headers = AzureEventHubsTransport.ReadHeaders(new Dictionary<string, object>
        {
            ["tenant"] = "acme",
            ["retries"] = 3L,
        });

        headers.ShouldNotBeNull();
        headers!["tenant"].ShouldBe("acme");
        headers["retries"].ShouldBe("3");
    }

    [Fact]
    public void The_partition_key_is_set_on_the_batch()
    {
        AzureEventHubsTransport.BatchOptions("customer-42").PartitionKey.ShouldBe("customer-42");
        AzureEventHubsTransport.BatchOptions(null).PartitionKey.ShouldBeNull();
    }

    [Fact]
    public async Task A_batch_is_split_where_the_partition_key_changes_and_keeps_the_original_order()
    {
        var sent = await SendInBatchesAsync(maxEvents: 10, ("a", "1"), ("a", "2"), ("b", "3"), (null, "4"), ("a", "5"));

        sent.ShouldBe(
        [
            ("a", "1,2"),
            ("b", "3"),
            (null, "4"),
            ("a", "5"),
        ]);
    }

    [Fact]
    public async Task A_full_batch_rolls_over_and_keeps_its_partition_key()
    {
        var sent = await SendInBatchesAsync(maxEvents: 2, ("a", "1"), ("a", "2"), ("a", "3"));

        sent.ShouldBe([("a", "1,2"), ("a", "3")]);
    }

    // Runs the transport's batching against fake batches that hold at most maxEvents, and returns each sent batch's
    // partition key and its bodies, comma-separated.
    private static async Task<List<(string? Key, string Bodies)>> SendInBatchesAsync(
        int maxEvents,
        params (string? Key, string Body)[] messages)
    {
        var sent = new List<(string?, string)>();
        var batches = new Dictionary<EventDataBatch, (string? Key, List<EventData> Store)>();

        await AzureEventHubsTransport.SendInBatchesAsync(
            "orders",
            [.. messages.Select(m => new TransportMessage("orders", m.Body) { PartitionKey = m.Key })],
            (options, _) =>
            {
                var store = new List<EventData>();
                var batch = EventHubsModelFactory.EventDataBatch(1024 * 1024, store, options, _ => store.Count < maxEvents);
                batches[batch] = (options.PartitionKey, store);
                return ValueTask.FromResult(batch);
            },
            (batch, _) =>
            {
                var (key, store) = batches[batch];
                sent.Add((key, string.Join(",", store.Select(e => e.EventBody.ToString()))));
                return Task.CompletedTask;
            },
            CancellationToken.None);

        return sent;
    }
}
