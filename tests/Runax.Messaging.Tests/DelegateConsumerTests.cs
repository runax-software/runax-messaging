using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class DelegateConsumerTests
{
    private sealed record OrderPlaced(int Id);

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task A_delegate_consumer_in_the_bus_block_receives_messages_on_its_topic()
    {
        var received = new TaskCompletionSource<(string Topic, OrderPlaced? Order)>();

        using var host = await StartAsync(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer("orders.placed", (context, _) =>
            {
                received.TrySetResult((context.Topic, context.Deserialize<OrderPlaced>()));
                return ValueTask.CompletedTask;
            });
        }));

        await host.Services.GetRequiredService<IBus>().PublishAsync("orders.placed", new OrderPlaced(42));

        var (topic, order) = await received.Task.WaitAsync(Timeout);
        topic.ShouldBe("orders.placed");
        order.ShouldBe(new OrderPlaced(42));
    }

    [Fact]
    public async Task A_failing_delegate_consumer_is_retried_per_the_bus_policy()
    {
        var attempts = 0;
        var succeeded = new TaskCompletionSource();

        using var host = await StartAsync(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.WithRetry(r =>
            {
                r.MaxAttempts = 3;
                r.InitialDelay = TimeSpan.FromMilliseconds(1);
            });
            bus.AddConsumer("orders.placed", (_, _) =>
            {
                if (Interlocked.Increment(ref attempts) < 3)
                    throw new InvalidOperationException("transient");

                succeeded.TrySetResult();
                return ValueTask.CompletedTask;
            });
        }));

        await host.Services.GetRequiredService<IBus>().PublishAsync("orders.placed", new OrderPlaced(1));

        await succeeded.Task.WaitAsync(Timeout);
        attempts.ShouldBe(3);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_consumer_added_outside_the_bus_block_receives_messages(bool busFirst)
    {
        var received = new TaskCompletionSource<OrderPlaced?>();

        using var host = await StartAsync(m =>
        {
            if (busFirst)
                AddBus(m);

            m.AddConsumer("main", "orders.placed", (context, _) =>
            {
                received.TrySetResult(context.Deserialize<OrderPlaced>());
                return ValueTask.CompletedTask;
            });

            if (!busFirst)
                AddBus(m);
        });

        await host.Services.GetRequiredKeyedService<IBus>("main").PublishAsync("orders.placed", new OrderPlaced(7));

        (await received.Task.WaitAsync(Timeout)).ShouldBe(new OrderPlaced(7));

        static void AddBus(MessagingConfigurator m) =>
            m.AddBus("main", bus => bus.AddTransport(new InMemoryConfig()));
    }

    [Fact]
    public async Task Consumers_from_the_block_and_from_outside_it_share_one_subscription()
    {
        var calls = new ConcurrentQueue<string>();
        var bothCalled = new TaskCompletionSource();

        ValueTask Record(string name)
        {
            calls.Enqueue(name);
            if (calls.Count == 2)
                bothCalled.TrySetResult();
            return ValueTask.CompletedTask;
        }

        using var host = await StartAsync(m =>
        {
            m.AddBus("main", bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.AddConsumer("orders.placed", (_, _) => Record("inside"));
            });
            m.AddConsumer("main", "orders.placed", (_, _) => Record("outside"));
        });

        host.Services.GetServices<IHostedService>().OfType<Consumers.MessageConsumerHostedService>().Count().ShouldBe(1);

        await host.Services.GetRequiredKeyedService<IBus>("main").PublishAsync("orders.placed", new OrderPlaced(1));
        await bothCalled.Task.WaitAsync(Timeout);
        await Task.Delay(50);

        calls.ToArray().ShouldBe(["inside", "outside"]);
    }

    [Fact]
    public async Task A_consumer_on_a_bus_that_was_never_added_fails_startup()
    {
        var ex = await Should.ThrowAsync<InvalidOperationException>(() => StartAsync(m =>
            m.AddConsumer("missing", "orders.placed", (_, _) => ValueTask.CompletedTask)));

        ex.Message.ShouldContain("'missing'");
    }

    [Fact]
    public async Task A_consumer_added_outside_a_PublishOnly_bus_fails_startup()
    {
        var ex = await Should.ThrowAsync<InvalidOperationException>(() => StartAsync(m =>
        {
            m.AddBus("audit", bus =>
            {
                bus.Mode = BusMode.PublishOnly;
                bus.AddTransport(new InMemoryConfig());
            });
            m.AddConsumer("audit", "orders.placed", (_, _) => ValueTask.CompletedTask);
        }));

        ex.Message.ShouldContain("PublishOnly");
        ex.Message.ShouldContain("a delegate consumer on topic 'orders.placed'");
    }

    [Fact]
    public void A_delegate_consumer_in_a_PublishOnly_bus_block_throws_when_the_block_completes()
    {
        var services = new ServiceCollection();

        var ex = Should.Throw<InvalidOperationException>(() => services.AddRunaxMessaging(m => m.AddBus("audit", bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer("orders.placed", (_, _) => ValueTask.CompletedTask);
            bus.Mode = BusMode.PublishOnly;
        })));

        ex.Message.ShouldContain("a delegate consumer on topic 'orders.placed'");
    }

    [Fact]
    public void An_empty_topic_is_rejected()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() => services.AddRunaxMessaging(m =>
            m.AddConsumer("main", "", (_, _) => ValueTask.CompletedTask)));
    }

    private static async Task<IHost> StartAsync(Action<MessagingConfigurator> configure)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRunaxMessaging(configure);

        var host = builder.Build();
        try
        {
            await host.StartAsync();
            return host;
        }
        catch
        {
            host.Dispose();
            throw;
        }
    }
}
