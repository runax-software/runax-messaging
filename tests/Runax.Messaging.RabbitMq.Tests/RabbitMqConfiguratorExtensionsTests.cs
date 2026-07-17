using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.RabbitMq;

namespace Runax.Messaging.RabbitMq.Tests;

public class RabbitMqConfiguratorExtensionsTests
{
    [Fact]
    public void AddRabbitMq_registers_the_transport_and_applies_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddRabbitMq(o =>
        {
            o.HostName = "broker.internal";
            o.ExchangeName = "custom.exchange";
        }));

        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<RabbitMqOptions>();
        options.HostName.ShouldBe("broker.internal");
        options.ExchangeName.ShouldBe("custom.exchange");

        provider.GetRequiredService<IMessagingTransport>().ShouldBeOfType<RabbitMqTransport>();
    }

    [Fact]
    public void AddRabbitMq_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddRabbitMq(_ => { });

        result.ShouldBeSameAs(configurator);
    }
}
