using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.RabbitMq.Tests;

public class RabbitMqConfigBindingTests
{
    [Fact]
    public void Binds_config_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RabbitMq:HostName"] = "broker.internal",
                ["RabbitMq:Port"] = "5671",
                ["RabbitMq:UseTls"] = "true",
                ["RabbitMq:Uri"] = "amqps://user:pass@broker.internal:5671/",
            })
            .Build();

        // The section-binding AddTransport overload uses the same Bind; assert the mapping directly.
        var config = new RabbitMqConfig();
        configuration.GetSection("RabbitMq").Bind(config);

        config.HostName.ShouldBe("broker.internal");
        config.Port.ShouldBe(5671);
        config.UseTls.ShouldBeTrue();
        config.Uri.ShouldBe("amqps://user:pass@broker.internal:5671/");

        // And the section overload registers a working transport.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport<RabbitMqConfig>(configuration.GetSection("RabbitMq"))));
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<RabbitMqTransport>();
    }

    [Fact]
    public void Invalid_port_fails_validation_at_configuration_time()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var exception = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
                bus.AddTransport(new RabbitMqConfig { Port = 70000 }))));

        exception.Message.ShouldContain("transport config is invalid");
    }
}
