using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class ServiceCollectionExtensionsTests
{
    private sealed record Ping(string Value);

    private sealed class PingConsumer : MessageConsumer<Ping>
    {
        public override string Topic => "ping";

        protected override ValueTask HandleAsync(Ping message, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }

    [Fact]
    public void AddRunaxMessaging_registers_the_bus_and_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new InMemoryConfig())));

        using var provider = services.BuildServiceProvider();
        provider.GetService<IBus>().ShouldNotBeNull();
        provider.GetKeyedService<IMessagingTransport>(BusNames.Default).ShouldNotBeNull();
    }

    [Fact]
    public void AddRunaxMessaging_without_consumers_registers_no_hosted_service()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new InMemoryConfig())));

        services.ShouldNotContain(d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void AddConsumer_registers_the_consumer_and_the_hosted_dispatcher()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<PingConsumer>();
        }));

        services.ShouldContain(d => d.ServiceType == typeof(PingConsumer));
        services.ShouldContain(d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void AddRunaxMessaging_returns_the_same_service_collection()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var result = services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new InMemoryConfig())));

        result.ShouldBeSameAs(services);
    }
}
