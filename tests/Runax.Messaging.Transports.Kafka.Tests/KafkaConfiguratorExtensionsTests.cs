using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Kafka.Tests;

public class KafkaConfiguratorExtensionsTests
{
    [Fact]
    public void AddBus_with_a_kafka_config_registers_the_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new KafkaConfig
        {
            BootstrapServers = "broker.internal:9092",
            ConsumerGroupId = "custom-group",
        })));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<KafkaTransport>();
        provider.GetRequiredService<IBus>().Name.ShouldBe(BusNames.Default);
    }

    [Fact]
    public void AddBus_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddBus(bus =>
            bus.AddTransport(new KafkaConfig { BootstrapServers = "localhost:9092" }));

        result.ShouldBeSameAs(configurator);
    }
}
