using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Outbox;

namespace Runax.Messaging.Outbox.Tests;

public class OutboxTests
{
    private sealed record Order(int Id);

    private sealed class OrderConsumer(TaskCompletionSource<Order> received) : MessageConsumer<Order>
    {
        public override string Topic => "orders";

        protected override ValueTask HandleAsync(Order message, CancellationToken cancellationToken)
        {
            received.TrySetResult(message);
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task Publish_writes_to_the_store_without_dispatching()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory().AddOutbox().AddInMemoryOutboxStore());
        using var provider = services.BuildServiceProvider();

        // No host started, so the dispatcher never runs: the publish only lands in the store.
        await provider.GetRequiredService<IMessagePublisher>().PublishAsync("orders", new Order(1));

        var store = provider.GetRequiredService<IOutboxStore>();
        var pending = await store.GetPendingAsync(10);
        pending.Count.ShouldBe(1);
        pending[0].Topic.ShouldBe("orders");
    }

    [Fact]
    public async Task Dispatcher_delivers_stored_messages_to_the_consumer()
    {
        var received = new TaskCompletionSource<Order>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddRunaxMessaging(m => m
            .AddInMemory()
            .AddConsumer<OrderConsumer>()
            .AddOutbox(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
            .AddInMemoryOutboxStore());
        using var host = builder.Build();
        await host.StartAsync();

        await host.Services.GetRequiredService<IMessagePublisher>().PublishAsync("orders", new Order(7));

        var order = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        order.Id.ShouldBe(7);

        // The dispatched message is marked so it is not published again.
        var store = host.Services.GetRequiredService<IOutboxStore>();
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while ((await store.GetPendingAsync(10)).Count > 0 && DateTime.UtcNow < deadline)
            await Task.Delay(25);

        (await store.GetPendingAsync(10)).ShouldBeEmpty();

        await host.StopAsync();
    }
}
