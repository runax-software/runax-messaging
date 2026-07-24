using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Kafka;

namespace Runax.Messaging.Transports.Kafka.Tests;

public class KafkaConfiguratorExtensionsTests
{
    [Fact]
    public void AddKafka_registers_the_transport_and_applies_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddKafka(kafka => kafka.Configure(o =>
        {
            o.BootstrapServers = "broker.internal:9092";
            o.ConsumerGroupId = "custom-group";
        })));

        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<KafkaOptions>();
        options.BootstrapServers.ShouldBe("broker.internal:9092");
        options.ConsumerGroupId.ShouldBe("custom-group");

        provider.GetRequiredService<IMessagingTransport>().ShouldBeOfType<KafkaTransport>();
    }

    [Fact]
    public void AddKafka_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddKafka(kafka => kafka.Configure(o => o.BootstrapServers = "localhost:9092"));

        result.ShouldBeSameAs(configurator);
    }
}
