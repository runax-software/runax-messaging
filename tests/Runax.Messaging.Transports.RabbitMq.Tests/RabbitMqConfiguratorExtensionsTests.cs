using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.RabbitMq.Tests;

public class RabbitMqConfiguratorExtensionsTests
{
    [Fact]
    public void AddBus_with_a_rabbitmq_config_registers_the_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new RabbitMqConfig
        {
            HostName = "broker.internal",
            ExchangeName = "custom.exchange",
        })));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<RabbitMqTransport>();
        provider.GetRequiredService<IBus>().Name.ShouldBe(BusNames.Default);
    }

    [Fact]
    public void AddBus_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddBus(bus => bus.AddTransport(new RabbitMqConfig()));

        result.ShouldBeSameAs(configurator);
    }
}
