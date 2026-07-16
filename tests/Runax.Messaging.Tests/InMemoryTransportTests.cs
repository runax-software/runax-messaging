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
            return ValueTask.CompletedTask;
        }, cts.Token);

        var result = await received.Task;
        result.Json.ShouldBe("envelope-1");
        result.Topic.ShouldBe("orders");

        await cts.CancelAsync();
        await subscription;
    }
}
