using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
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
        services.AddRunaxMessaging(m => m.AddBus(bus => bus
            .AddTransport(new InMemoryConfig())
            .AddOutbox()
            .AddOutboxStore(new InMemoryOutboxStoreConfig())));
        using var provider = services.BuildServiceProvider();

        // No host started, so the dispatcher never runs: the publish only lands in the store.
        await provider.GetRequiredService<IBus>().PublishAsync("orders", new Order(1));

        var store = provider.GetRequiredKeyedService<IOutboxStore>(BusNames.Default);
        var pending = await store.GetPendingAsync(BusNames.Default, 10);
        pending.Count.ShouldBe(1);
        pending[0].Topic.ShouldBe("orders");
    }

    [Fact]
    public async Task Dispatcher_delivers_stored_messages_to_the_consumer()
    {
        var received = new TaskCompletionSource<Order>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus => bus
            .AddTransport(new InMemoryConfig())
            .AddConsumer<OrderConsumer>()
            .AddOutbox(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
            .AddOutboxStore(new InMemoryOutboxStoreConfig())));
        using var host = builder.Build();
        await host.StartAsync();

        await host.Services.GetRequiredService<IBus>().PublishAsync("orders", new Order(7));

        var order = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        order.Id.ShouldBe(7);

        // The dispatched message is marked so it is not published again.
        var store = host.Services.GetRequiredKeyedService<IOutboxStore>(BusNames.Default);
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while ((await store.GetPendingAsync(BusNames.Default, 10)).Count > 0 && DateTime.UtcNow < deadline)
            await Task.Delay(25);

        (await store.GetPendingAsync(BusNames.Default, 10)).ShouldBeEmpty();

        await host.StopAsync();
    }

    [Fact]
    public async Task Dispatcher_publishes_the_stored_headers_as_native_headers()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus => bus
            .AddTransport(new InMemoryConfig())
            .AddOutbox(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
            .AddOutboxStore(new InMemoryOutboxStoreConfig())));
        using var host = builder.Build();
        await host.StartAsync();

        // Read the topic straight off the transport to see what the dispatcher sent.
        var transport = host.Services.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);
        var delivered = new TaskCompletionSource<TransportMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = new CancellationTokenSource();
        var listener = transport.SubscribeAsync(["orders"], message =>
        {
            delivered.TrySetResult(message);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, stop.Token);

        await host.Services.GetRequiredService<IBus>().PublishAsync(
            "orders", new Order(7), new Dictionary<string, string> { ["tenant"] = "acme" });

        var message = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        message.Headers.ShouldContainKeyAndValue("tenant", "acme");
        message.Body.ShouldContain("\"tenant\":\"acme\"");

        await stop.CancelAsync();
        await listener;
        await host.StopAsync();
    }

    [Fact]
    public async Task Invalid_outbox_options_throw_on_host_start()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus => bus
            .AddTransport(new InMemoryConfig())
            .AddOutbox(o => o.BatchSize = 0)
            .AddOutboxStore(new InMemoryOutboxStoreConfig())));
        using var host = builder.Build();

        var ex = await Should.ThrowAsync<OptionsValidationException>(host.StartAsync());
        ex.Message.ShouldContain(nameof(OutboxOptions.BatchSize));
    }
}
