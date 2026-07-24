using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Transports.Google.PubSub;

namespace Runax.Messaging.Transports.Google.PubSub.Tests;

public class GooglePubSubConfiguratorTests
{
    [Fact]
    public void Options_defaults_are_sensible()
    {
        var options = new GooglePubSubOptions();

        options.ProjectId.ShouldBe(string.Empty);
        options.TopicSubscriptionMap.ShouldBeEmpty();
    }

    [Fact]
    public void AddGooglePubSub_registers_the_transport_and_applies_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddGooglePubSub(__tb => __tb.Configure(o => o.ProjectId = "my-project")));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<GooglePubSubOptions>().ProjectId.ShouldBe("my-project");
        provider.GetRequiredService<IMessagingTransport>().ShouldBeOfType<GooglePubSubTransport>();
    }

    [Fact]
    public void AddGooglePubSub_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddGooglePubSub(__tb => __tb.Configure(o => o.ProjectId = "p"));

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_options_from_configuration()
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
        services.AddRunaxMessaging(m => m.AddGooglePubSub(configuration.GetSection("PubSub")));
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<GooglePubSubOptions>();
        options.ProjectId.ShouldBe("bound-project");
        options.TopicSubscriptionMap["orders"].ShouldBe("orders-sub");
    }

    [Fact]
    public void Missing_project_id_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddGooglePubSub(__tb => __tb.Configure(_ => { })));
        using var provider = services.BuildServiceProvider();

        Should.Throw<OptionsValidationException>(() => provider.GetRequiredService<GooglePubSubOptions>());
    }

    [Fact]
    public async Task Health_check_reports_unhealthy_when_the_transport_is_not_pubsub()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        services.AddHealthChecks().AddGooglePubSubTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Unhealthy);
        report.Entries["google-pubsub"].Description.ShouldNotBeNull().ShouldContain("not Google Pub/Sub");
    }
}
