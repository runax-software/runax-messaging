using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Transports.Azure.EventHubs;

namespace Runax.Messaging.Transports.Azure.EventHubs.Tests;

public class AzureEventHubsConfiguratorTests
{
    private const string Namespace = "test.servicebus.windows.net";

    [Fact]
    public void Options_defaults_are_sensible()
    {
        var options = new AzureEventHubsOptions();

        options.FullyQualifiedNamespace.ShouldBeNull();
        options.ConnectionString.ShouldBeNull();
        options.ConsumerGroup.ShouldBe("$Default");
        options.BlobConnectionString.ShouldBeNull();
        options.BlobContainerName.ShouldBeNull();
        options.ProduceDeadLetterHub.ShouldBeFalse();
    }

    [Fact]
    public void AddAzureEventHubs_registers_the_transport_and_applies_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddAzureEventHubs(eventHubs =>
            eventHubs.Configure(o => o.FullyQualifiedNamespace = Namespace)));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<AzureEventHubsOptions>().FullyQualifiedNamespace.ShouldBe(Namespace);
        provider.GetRequiredService<IMessagingTransport>().ShouldBeOfType<AzureEventHubsTransport>();
    }

    [Fact]
    public void AddAzureEventHubs_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddAzureEventHubs(eventHubs =>
            eventHubs.Configure(o => o.FullyQualifiedNamespace = Namespace));

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_options_from_configuration()
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
        services.AddRunaxMessaging(m => m.AddAzureEventHubs(configuration.GetSection("EventHubs")));
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<AzureEventHubsOptions>();
        options.FullyQualifiedNamespace.ShouldBe(Namespace);
        options.ConsumerGroup.ShouldBe("orders-worker");
        options.BlobConnectionString.ShouldBe("UseDevelopmentStorage=true");
        options.BlobContainerName.ShouldBe("checkpoints");
        options.ProduceDeadLetterHub.ShouldBeTrue();
    }

    [Fact]
    public void Missing_consumer_group_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddAzureEventHubs(eventHubs =>
            eventHubs.Configure(o => o.ConsumerGroup = string.Empty)));
        using var provider = services.BuildServiceProvider();

        Should.Throw<OptionsValidationException>(() => provider.GetRequiredService<AzureEventHubsOptions>());
    }

    [Fact]
    public void Missing_connection_info_throws_when_transport_is_constructed()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddAzureEventHubs(eventHubs => eventHubs.Configure(_ => { })));
        using var provider = services.BuildServiceProvider();

        // ConsumerGroup has a default, so options validation passes; the connection guard runs in the ctor.
        Should.Throw<ValidationException>(() => provider.GetRequiredService<IMessagingTransport>());
    }

    [Fact]
    public async Task Health_check_reports_unhealthy_when_the_transport_is_not_event_hubs()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        services.AddHealthChecks().AddAzureEventHubsTransport("orders.placed");
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Unhealthy);
        report.Entries["azure-eventhubs"].Description.ShouldNotBeNull().ShouldContain("not Azure Event Hubs");
    }
}
