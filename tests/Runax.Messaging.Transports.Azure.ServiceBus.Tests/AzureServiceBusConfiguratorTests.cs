using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.ServiceBus.Tests;

public class AzureServiceBusConfiguratorTests
{
    private const string ConnectionString =
        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123";

    [Fact]
    public void Config_defaults_are_sensible()
    {
        var config = new AzureServiceBusConfig();

        config.ConnectionString.ShouldBe(string.Empty);
        config.TopicSubscriptionMap.ShouldBeEmpty();
        config.MaxConcurrentCalls.ShouldBe(1);
    }

    [Fact]
    public void AddBus_registers_the_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new AzureServiceBusConfig { ConnectionString = ConnectionString })));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<AzureServiceBusTransport>();
    }

    [Fact]
    public void AddBus_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddBus(bus =>
            bus.AddTransport(new AzureServiceBusConfig { ConnectionString = ConnectionString }));

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_config_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ServiceBus:ConnectionString"] = ConnectionString,
                ["ServiceBus:MaxConcurrentCalls"] = "8",
                ["ServiceBus:TopicSubscriptionMap:orders"] = "orders-sub",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport<AzureServiceBusConfig>(configuration.GetSection("ServiceBus"))));
        using var provider = services.BuildServiceProvider();

        // The bound ConnectionString satisfied [Required] validation and the transport resolves.
        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<AzureServiceBusTransport>();

        // The section shape binds every property onto the config type (the same binding
        // AddTransport<TConfig>(IConfiguration) performs).
        var config = new AzureServiceBusConfig();
        configuration.GetSection("ServiceBus").Bind(config);
        config.ConnectionString.ShouldBe(ConnectionString);
        config.MaxConcurrentCalls.ShouldBe(8);
        config.TopicSubscriptionMap["orders"].ShouldBe("orders-sub");
    }

    [Fact]
    public void Missing_connection_string_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var exception = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
                bus.AddTransport(new AzureServiceBusConfig()))));

        exception.Message.ShouldContain("transport config is invalid");
    }

    [Fact]
    public void Health_check_is_auto_registered_for_the_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new AzureServiceBusConfig { ConnectionString = ConnectionString })));
        using var provider = services.BuildServiceProvider();

        var registrations = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        registrations.ShouldContain(r => r.Name == $"runax:{BusNames.Default}");
    }

    [Fact]
    public async Task Health_check_reports_unhealthy_when_the_transport_is_not_service_bus()
    {
        // The 2.0 auto-registered check is always wired to its own bus's transport, so the
        // mismatch can only be produced by constructing the check against a foreign transport.
        var check = new AzureServiceBusHealthCheck(Substitute.For<IMessagingTransport>());

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldNotBeNull().ShouldContain("not Azure Service Bus");
    }
}
