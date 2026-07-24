using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Runax.Messaging.Transports.Kafka;

namespace Runax.Messaging.Transports.Kafka.Tests;

public class KafkaOptionsBindingTests
{
    [Fact]
    public void Binds_options_from_configuration()
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

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddKafka(configuration.GetSection("Kafka")));
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<KafkaOptions>();
        options.BootstrapServers.ShouldBe("broker.internal:9092");
        options.ConsumerGroupId.ShouldBe("orders");
        options.AutoOffsetReset.ShouldBe("latest");
        options.EnableIdempotence.ShouldBeFalse();
    }

    [Fact]
    public void Missing_bootstrap_servers_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddKafka(kafka => kafka.Configure(o => o.BootstrapServers = string.Empty)));
        using var provider = services.BuildServiceProvider();

        Should.Throw<OptionsValidationException>(() => provider.GetRequiredService<KafkaOptions>());
    }
}
