using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sqs.Tests;

public class SqsConfiguratorExtensionsTests
{
    [Fact]
    public void AddBus_with_an_sqs_config_registers_the_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new SqsConfig
        {
            Region = "eu-west-1",
            ServiceUrl = "http://localhost:4566",
            AccessKey = "test",
            SecretKey = "test",
        })));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<SqsTransport>();
        provider.GetRequiredService<IBus>().Name.ShouldBe(BusNames.Default);
    }

    [Fact]
    public void AddBus_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddBus(bus => bus.AddTransport(new SqsConfig()));

        result.ShouldBeSameAs(configurator);
    }
}
