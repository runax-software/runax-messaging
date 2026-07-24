using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Transports.Redis;

namespace Runax.Messaging.Transports.Redis.Tests;

public class RedisConfiguratorTests
{
    [Fact]
    public void Options_defaults_are_sensible()
    {
        var options = new RedisOptions();

        options.Configuration.ShouldBe(string.Empty);
        options.ConsumerGroup.ShouldBe("runax");
        options.ConsumerName.ShouldNotBeNullOrWhiteSpace();
        options.ReadBatchSize.ShouldBe(10);
        options.PollInterval.ShouldBe(TimeSpan.FromSeconds(1));
        options.ClaimIdleTime.ShouldBe(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void AddRedis_registers_the_transport_and_applies_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddRedis(__tb => __tb.Configure(o =>
        {
            o.Configuration = "localhost:6379";
            o.ConsumerGroup = "workers";
        })));

        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<RedisOptions>();
        options.Configuration.ShouldBe("localhost:6379");
        options.ConsumerGroup.ShouldBe("workers");
        provider.GetRequiredService<IMessagingTransport>().ShouldBeOfType<RedisTransport>();
    }

    [Fact]
    public void AddRedis_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddRedis(__tb => __tb.Configure(o => o.Configuration = "localhost:6379"));

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_options_from_configuration()
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
        services.AddRunaxMessaging(m => m.AddRedis(configuration.GetSection("Redis")));
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<RedisOptions>();
        options.Configuration.ShouldBe("redis.internal:6380");
        options.ConsumerGroup.ShouldBe("bound-group");
        options.ReadBatchSize.ShouldBe(25);
    }

    [Fact]
    public void Missing_configuration_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddRedis(__tb => __tb.Configure(_ => { })));
        using var provider = services.BuildServiceProvider();

        Should.Throw<OptionsValidationException>(() => provider.GetRequiredService<RedisOptions>());
    }

    [Fact]
    public async Task Health_check_reports_unhealthy_when_the_transport_is_not_redis()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        services.AddHealthChecks().AddRedisTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Unhealthy);
        report.Entries["redis"].Description.ShouldNotBeNull().ShouldContain("not Redis");
    }
}
