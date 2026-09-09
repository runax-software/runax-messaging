using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class BusModeTests
{
    private sealed record Ping(string Value);

    private sealed class PingConsumer : MessageConsumer<Ping>
    {
        public override string Topic => "ping";

        protected override ValueTask HandleAsync(Ping message, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }

    private static ServiceCollection NewServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        return services;
    }

    [Fact]
    public void The_default_mode_is_PublishAndConsume()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new InMemoryConfig())));

        using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IBus>().Mode.ShouldBe(BusMode.PublishAndConsume);
    }

    [Fact]
    public void Registering_a_consumer_on_a_PublishOnly_bus_throws_when_the_block_completes()
    {
        var services = NewServices();

        var ex = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus("audit", bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.AddConsumer<PingConsumer>();
                bus.Mode = BusMode.PublishOnly; // set last on purpose: order must not matter
            })));

        ex.Message.ShouldContain("PublishOnly");
        ex.Message.ShouldContain(nameof(PingConsumer));
    }

    [Fact]
    public void Configuring_a_consume_side_policy_on_a_PublishOnly_bus_throws()
    {
        var services = NewServices();

        var ex = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus("audit", bus =>
            {
                bus.Mode = BusMode.PublishOnly;
                bus.AddTransport(new InMemoryConfig());
                bus.WithRetry(o => o.MaxAttempts = 2);
            })));

        ex.Message.ShouldContain("PublishOnly");
        ex.Message.ShouldContain("WithRetry");
    }

    [Fact]
    public async Task Publishing_on_a_ConsumeOnly_bus_throws_at_the_publish_call()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m => m.AddBus("partner-feed", bus =>
        {
            bus.Mode = BusMode.ConsumeOnly;
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<PingConsumer>();
        }));

        await using var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredKeyedService<IBus>("partner-feed");

        var ex = await Should.ThrowAsync<InvalidOperationException>(async () =>
            await bus.PublishAsync("ping", new Ping("nope")));

        ex.Message.ShouldContain("'partner-feed'");
        ex.Message.ShouldContain("ConsumeOnly");
    }

    [Fact]
    public async Task Batch_publishing_on_a_ConsumeOnly_bus_throws_too()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m => m.AddBus("partner-feed", bus =>
        {
            bus.Mode = BusMode.ConsumeOnly;
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<PingConsumer>();
        }));

        await using var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredKeyedService<IBus>("partner-feed");

        await Should.ThrowAsync<InvalidOperationException>(async () =>
            await bus.PublishBatchAsync("ping", [new Ping("a"), new Ping("b")]));
    }

    [Fact]
    public void A_PublishOnly_bus_registers_no_consumer_hosted_service()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m => m.AddBus("audit", bus =>
        {
            bus.Mode = BusMode.PublishOnly;
            bus.AddTransport(new InMemoryConfig());
        }));

        services.ShouldNotContain(d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void The_declared_mode_is_exposed_on_the_bus()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus("write", bus =>
            {
                bus.Mode = BusMode.PublishOnly;
                bus.AddTransport(new InMemoryConfig());
            });
            m.AddBus("read", bus =>
            {
                bus.Mode = BusMode.ConsumeOnly;
                bus.AddTransport(new InMemoryConfig());
            });
        });

        using var provider = services.BuildServiceProvider();
        provider.GetRequiredKeyedService<IBus>("write").Mode.ShouldBe(BusMode.PublishOnly);
        provider.GetRequiredKeyedService<IBus>("read").Mode.ShouldBe(BusMode.ConsumeOnly);
    }
}
