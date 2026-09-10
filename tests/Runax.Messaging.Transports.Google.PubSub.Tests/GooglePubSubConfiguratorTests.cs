using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Google.PubSub.Tests;

public class GooglePubSubConfiguratorTests
{
    [Fact]
    public void Config_defaults_are_sensible()
    {
        var config = new GooglePubSubConfig();

        config.ProjectId.ShouldBe(string.Empty);
        config.TopicSubscriptionMap.ShouldBeEmpty();
    }

    [Fact]
    public void AddBus_registers_the_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new GooglePubSubConfig { ProjectId = "my-project" })));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<GooglePubSubTransport>();
    }

    [Fact]
    public void AddBus_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddBus(bus =>
            bus.AddTransport(new GooglePubSubConfig { ProjectId = "p" }));

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_config_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["PubSub:ProjectId"] = "bound-project",
                ["PubSub:TopicSubscriptionMap:orders"] = "orders-sub",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport<GooglePubSubConfig>(configuration.GetSection("PubSub"))));
        using var provider = services.BuildServiceProvider();

        // The bound ProjectId satisfied [Required] validation and the transport resolves.
        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<GooglePubSubTransport>();

        // The section shape binds every property onto the config type (the same binding
        // AddTransport<TConfig>(IConfiguration) performs).
        var config = new GooglePubSubConfig();
        configuration.GetSection("PubSub").Bind(config);
        config.ProjectId.ShouldBe("bound-project");
        config.TopicSubscriptionMap["orders"].ShouldBe("orders-sub");
    }

    [Fact]
    public void Missing_project_id_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var exception = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
                bus.AddTransport(new GooglePubSubConfig()))));

        exception.Message.ShouldContain("transport config is invalid");
    }

    [Fact]
    public void Health_check_is_auto_registered_for_the_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new GooglePubSubConfig { ProjectId = "my-project" })));
        using var provider = services.BuildServiceProvider();

        var registrations = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        registrations.ShouldContain(r => r.Name == $"runax:{BusNames.Default}");
    }

    [Fact]
    public async Task Health_check_reports_unhealthy_when_the_transport_is_not_pubsub()
    {
        // The 2.0 auto-registered check is always wired to its own bus's transport, so the
        // mismatch can only be produced by constructing the check against a foreign transport.
        var check = new GooglePubSubHealthCheck(Substitute.For<IMessagingTransport>());

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldNotBeNull().ShouldContain("not Google Pub/Sub");
    }
}
