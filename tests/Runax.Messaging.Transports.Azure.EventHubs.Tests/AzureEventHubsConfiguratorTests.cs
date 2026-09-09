using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.EventHubs.Tests;

public class AzureEventHubsConfiguratorTests
{
    private const string Namespace = "test.servicebus.windows.net";

    [Fact]
    public void Config_defaults_are_sensible()
    {
        var config = new AzureEventHubsConfig();

        config.FullyQualifiedNamespace.ShouldBeNull();
        config.ConnectionString.ShouldBeNull();
        config.ConsumerGroup.ShouldBe("$Default");
        config.BlobConnectionString.ShouldBeNull();
        config.BlobContainerName.ShouldBeNull();
        config.ProduceDeadLetterHub.ShouldBeFalse();
    }

    [Fact]
    public void AddBus_registers_the_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new AzureEventHubsConfig { FullyQualifiedNamespace = Namespace })));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<AzureEventHubsTransport>();
    }

    [Fact]
    public void AddBus_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddBus(bus =>
            bus.AddTransport(new AzureEventHubsConfig { FullyQualifiedNamespace = Namespace }));

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_config_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["EventHubs:FullyQualifiedNamespace"] = Namespace,
                ["EventHubs:ConsumerGroup"] = "orders-worker",
                ["EventHubs:BlobConnectionString"] = "UseDevelopmentStorage=true",
                ["EventHubs:BlobContainerName"] = "checkpoints",
                ["EventHubs:ProduceDeadLetterHub"] = "true",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport<AzureEventHubsConfig>(configuration.GetSection("EventHubs"))));
        using var provider = services.BuildServiceProvider();

        // The bound namespace satisfied the transport's connection guard and the transport resolves.
        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<AzureEventHubsTransport>();

        // The section shape binds every property onto the config type (the same binding
        // AddTransport<TConfig>(IConfiguration) performs).
        var config = new AzureEventHubsConfig();
        configuration.GetSection("EventHubs").Bind(config);
        config.FullyQualifiedNamespace.ShouldBe(Namespace);
        config.ConsumerGroup.ShouldBe("orders-worker");
        config.BlobConnectionString.ShouldBe("UseDevelopmentStorage=true");
        config.BlobContainerName.ShouldBe("checkpoints");
        config.ProduceDeadLetterHub.ShouldBeTrue();
    }

    [Fact]
    public void Missing_consumer_group_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var exception = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
                bus.AddTransport(new AzureEventHubsConfig { ConsumerGroup = string.Empty }))));

        exception.Message.ShouldContain("transport config is invalid");
    }

    [Fact]
    public void Missing_connection_info_throws_when_transport_is_constructed()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new AzureEventHubsConfig())));
        using var provider = services.BuildServiceProvider();

        // ConsumerGroup has a default, so DataAnnotations validation passes at AddBus time;
        // the connection guard runs in the transport's ctor on first resolution.
        Should.Throw<ValidationException>(() =>
            provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default));
    }

    [Fact]
    public void Health_check_is_auto_registered_for_the_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new AzureEventHubsConfig { FullyQualifiedNamespace = Namespace })));
        using var provider = services.BuildServiceProvider();

        var registrations = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        registrations.ShouldContain(r => r.Name == $"runax:{BusNames.Default}");
    }

    [Fact]
    public async Task Health_check_reports_unhealthy_when_the_transport_is_not_event_hubs()
    {
        // The 2.0 auto-registered check is always wired to its own bus's transport, so the
        // mismatch can only be produced by constructing the check against a foreign transport.
        var check = new AzureEventHubsHealthCheck(Substitute.For<IMessagingTransport>());

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldNotBeNull().ShouldContain("not Azure Event Hubs");
    }
}
