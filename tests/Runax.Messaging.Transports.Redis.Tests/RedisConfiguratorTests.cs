using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Redis.Tests;

public class RedisConfiguratorTests
{
    [Fact]
    public void Config_defaults_are_sensible()
    {
        var config = new RedisConfig();

        config.Configuration.ShouldBe(string.Empty);
        config.ConsumerGroup.ShouldBe("runax");
        config.ConsumerName.ShouldNotBeNullOrWhiteSpace();
        config.ReadBatchSize.ShouldBe(10);
        config.PollInterval.ShouldBe(TimeSpan.FromSeconds(1));
        config.ClaimIdleTime.ShouldBe(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void AddBus_registers_the_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new RedisConfig
        {
            Configuration = "localhost:6379",
            ConsumerGroup = "workers",
        })));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<RedisTransport>();
    }

    [Fact]
    public void AddBus_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddBus(bus =>
            bus.AddTransport(new RedisConfig { Configuration = "localhost:6379" }));

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_config_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Redis:Configuration"] = "redis.internal:6380",
                ["Redis:ConsumerGroup"] = "bound-group",
                ["Redis:ReadBatchSize"] = "25",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport<RedisConfig>(configuration.GetSection("Redis"))));
        using var provider = services.BuildServiceProvider();

        // The bound Configuration satisfied [Required] validation and the transport resolves.
        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<RedisTransport>();

        // The section shape binds every property onto the config type (the same binding
        // AddTransport<TConfig>(IConfiguration) performs).
        var config = new RedisConfig();
        configuration.GetSection("Redis").Bind(config);
        config.Configuration.ShouldBe("redis.internal:6380");
        config.ConsumerGroup.ShouldBe("bound-group");
        config.ReadBatchSize.ShouldBe(25);
    }

    [Fact]
    public void Missing_configuration_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var exception = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
                bus.AddTransport(new RedisConfig()))));

        exception.Message.ShouldContain("transport config is invalid");
    }

    [Fact]
    public void Health_check_is_auto_registered_for_the_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new RedisConfig { Configuration = "localhost:6379" })));
        using var provider = services.BuildServiceProvider();

        var registrations = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        registrations.ShouldContain(r => r.Name == $"runax:{BusNames.Default}");
    }

    [Fact]
    public async Task Health_check_reports_unhealthy_when_the_transport_is_not_redis()
    {
        // The 2.0 auto-registered check is always wired to its own bus's transport, so the
        // mismatch can only be produced by constructing the check against a foreign transport.
        var check = new RedisHealthCheck(Substitute.For<IMessagingTransport>());

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldNotBeNull().ShouldContain("not Redis");
    }
}
