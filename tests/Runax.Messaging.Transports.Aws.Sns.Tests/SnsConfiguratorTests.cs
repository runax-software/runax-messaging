using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sns.Tests;

public class SnsConfiguratorTests
{
    [Fact]
    public void Config_defaults_are_sensible()
    {
        var config = new SnsConfig();

        config.Region.ShouldBe("us-east-1");
        config.TopicArnMap.ShouldBeEmpty();
        config.TopicQueueUrlMap.ShouldBeEmpty();
        config.MaxNumberOfMessages.ShouldBe(10);
        config.WaitTimeSeconds.ShouldBe(20);
    }

    [Fact]
    public void AddBus_with_an_sns_config_registers_the_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new SnsConfig { Region = "eu-west-1" })));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<SnsTransport>();
        provider.GetRequiredService<IBus>().Name.ShouldBe(BusNames.Default);
    }

    [Fact]
    public void AddBus_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddBus(bus => bus.AddTransport(new SnsConfig()));

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_config_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Sns:Region"] = "ap-southeast-2",
                ["Sns:TopicQueueUrlMap:orders"] = "https://sqs/orders",
            })
            .Build();

        // The section-binding AddTransport overload uses the same Bind; assert the mapping directly.
        var config = new SnsConfig();
        configuration.GetSection("Sns").Bind(config);

        config.Region.ShouldBe("ap-southeast-2");
        config.TopicQueueUrlMap["orders"].ShouldBe("https://sqs/orders");

        // And the section overload registers a working transport.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport<SnsConfig>(configuration.GetSection("Sns"))));
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<SnsTransport>();
    }

    [Fact]
    public void Out_of_range_message_count_fails_validation_at_configuration_time()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var exception = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
                bus.AddTransport(new SnsConfig { MaxNumberOfMessages = 50 }))));

        exception.Message.ShouldContain("transport config is invalid");
    }

    [Fact]
    public void Health_check_is_auto_registered_for_the_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new SnsConfig())));
        using var provider = services.BuildServiceProvider();

        var registrations = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        registrations.ShouldContain(r => r.Name == $"runax:{BusNames.Default}");
    }

    [Fact]
    public void Health_check_is_not_registered_when_disabled()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new SnsConfig { RegisterHealthCheck = false })));
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations.ShouldBeEmpty();
    }
}
