using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Kafka.Tests;

public class KafkaConfigBindingTests
{
    [Fact]
    public void Binds_config_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:BootstrapServers"] = "broker.internal:9092",
                ["Kafka:ConsumerGroupId"] = "orders",
                ["Kafka:AutoOffsetReset"] = "latest",
                ["Kafka:EnableIdempotence"] = "false",
            })
            .Build();

        // The section-binding AddTransport overload uses the same Bind; assert the mapping directly.
        var config = new KafkaConfig();
        configuration.GetSection("Kafka").Bind(config);

        config.BootstrapServers.ShouldBe("broker.internal:9092");
        config.ConsumerGroupId.ShouldBe("orders");
        config.AutoOffsetReset.ShouldBe("latest");
        config.EnableIdempotence.ShouldBeFalse();

        // And the section overload registers a working transport.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport<KafkaConfig>(configuration.GetSection("Kafka"))));
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<KafkaTransport>();
    }

    [Fact]
    public void Missing_bootstrap_servers_fails_validation_at_configuration_time()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var exception = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
                bus.AddTransport(new KafkaConfig { BootstrapServers = string.Empty }))));

        exception.Message.ShouldContain("transport config is invalid");
    }
}
