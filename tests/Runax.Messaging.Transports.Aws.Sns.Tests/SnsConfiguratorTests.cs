using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Transports.Aws.Sns;

namespace Runax.Messaging.Transports.Aws.Sns.Tests;

public class SnsConfiguratorTests
{
    [Fact]
    public void Options_defaults_are_sensible()
    {
        var options = new SnsOptions();

        options.Region.ShouldBe("us-east-1");
        options.TopicArnMap.ShouldBeEmpty();
        options.TopicQueueUrlMap.ShouldBeEmpty();
        options.MaxNumberOfMessages.ShouldBe(10);
        options.WaitTimeSeconds.ShouldBe(20);
    }

    [Fact]
    public void AddSns_registers_the_transport_and_applies_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSns(o => o.Region = "eu-west-1"));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<SnsOptions>().Region.ShouldBe("eu-west-1");
        provider.GetRequiredService<IMessagingTransport>().ShouldBeOfType<SnsTransport>();
    }

    [Fact]
    public void AddSns_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddSns(_ => { });

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_options_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Sns:Region"] = "ap-southeast-2",
                ["Sns:TopicQueueUrlMap:orders"] = "https://sqs/orders",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSns(configuration.GetSection("Sns")));
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<SnsOptions>();
        options.Region.ShouldBe("ap-southeast-2");
        options.TopicQueueUrlMap["orders"].ShouldBe("https://sqs/orders");
    }

    [Fact]
    public void Out_of_range_message_count_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSns(o => o.MaxNumberOfMessages = 50));
        using var provider = services.BuildServiceProvider();

        Should.Throw<OptionsValidationException>(() => provider.GetRequiredService<SnsOptions>());
    }

    [Fact]
    public async Task Health_check_reports_unhealthy_when_the_transport_is_not_sns()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        services.AddHealthChecks().AddSnsTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Unhealthy);
        report.Entries["sns"].Description.ShouldNotBeNull().ShouldContain("not SNS");
    }
}
