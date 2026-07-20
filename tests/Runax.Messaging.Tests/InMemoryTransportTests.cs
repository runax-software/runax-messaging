using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class InMemoryTransportTests
{
    [Fact]
    public async Task Publish_then_Subscribe_delivers_the_message()
    {
        var transport = new InMemoryTransport();
        await transport.PublishAsync("orders", "envelope-1");

        var received = new TaskCompletionSource<(string Json, string Topic)>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var subscription = transport.SubscribeAsync(["orders"], (json, topic) =>
        {
            received.TrySetResult((json, topic));
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        var result = await received.Task;
        result.Json.ShouldBe("envelope-1");
        result.Topic.ShouldBe("orders");

        await cts.CancelAsync();
        await subscription;
    }

    [Fact]
    public async Task Subscribe_delivers_multiple_messages_in_order()
    {
        var transport = new InMemoryTransport();
        await transport.PublishAsync("orders", "a");
        await transport.PublishAsync("orders", "b");
        await transport.PublishAsync("orders", "c");

        var received = new List<string>();
        var done = new TaskCompletionSource();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var subscription = transport.SubscribeAsync(["orders"], (json, _) =>
        {
            received.Add(json);
            if (received.Count == 3)
                done.TrySetResult();
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        await done.Task;
        received.ShouldBe(["a", "b", "c"]);

        await cts.CancelAsync();
        await subscription;
    }

    [Fact]
    public async Task Subscribe_only_receives_messages_for_its_topics()
    {
        var transport = new InMemoryTransport();
        await transport.PublishAsync("orders", "for-orders");
        await transport.PublishAsync("shipments", "for-shipments");

        var received = new TaskCompletionSource<(string Json, string Topic)>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var subscription = transport.SubscribeAsync(["orders"], (json, topic) =>
        {
            received.TrySetResult((json, topic));
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        var result = await received.Task;
        result.Topic.ShouldBe("orders");
        result.Json.ShouldBe("for-orders");

        await cts.CancelAsync();
        await subscription;
    }

    [Fact]
    public async Task Subscribe_completes_when_cancelled()
    {
        var transport = new InMemoryTransport();
        using var cts = new CancellationTokenSource();

        var subscription = transport.SubscribeAsync(["orders"], (_, _) => ValueTask.FromResult(MessageDisposition.Acknowledge), cts.Token);
        await cts.CancelAsync();

        await Should.NotThrowAsync(subscription);
    }
}
