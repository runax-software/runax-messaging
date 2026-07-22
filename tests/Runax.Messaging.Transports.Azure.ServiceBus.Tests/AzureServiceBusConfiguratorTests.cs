using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Transports.Azure.ServiceBus;

namespace Runax.Messaging.Transports.Azure.ServiceBus.Tests;

public class AzureServiceBusConfiguratorTests
{
    private const string ConnectionString =
        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123";

    [Fact]
    public void Options_defaults_are_sensible()
    {
        var options = new AzureServiceBusOptions();

        options.ConnectionString.ShouldBe(string.Empty);
        options.TopicSubscriptionMap.ShouldBeEmpty();
        options.MaxConcurrentCalls.ShouldBe(1);
    }

    [Fact]
    public void AddAzureServiceBus_registers_the_transport_and_applies_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddAzureServiceBus(o => o.ConnectionString = ConnectionString));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<AzureServiceBusOptions>().ConnectionString.ShouldBe(ConnectionString);
        provider.GetRequiredService<IMessagingTransport>().ShouldBeOfType<AzureServiceBusTransport>();
    }

    [Fact]
    public void AddAzureServiceBus_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddAzureServiceBus(o => o.ConnectionString = ConnectionString);

        result.ShouldBeSameAs(configurator);
    }

    [Fact]
    public void Binds_options_from_configuration()
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
        services.AddRunaxMessaging(m => m.AddAzureServiceBus(configuration.GetSection("ServiceBus")));
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<AzureServiceBusOptions>();
        options.MaxConcurrentCalls.ShouldBe(8);
        options.TopicSubscriptionMap["orders"].ShouldBe("orders-sub");
    }

    [Fact]
    public void Missing_connection_string_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddAzureServiceBus(_ => { }));
        using var provider = services.BuildServiceProvider();

        Should.Throw<OptionsValidationException>(() => provider.GetRequiredService<AzureServiceBusOptions>());
    }

    [Fact]
    public async Task Health_check_reports_unhealthy_when_the_transport_is_not_service_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        services.AddHealthChecks().AddAzureServiceBusTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Unhealthy);
        report.Entries["azure-servicebus"].Description.ShouldNotBeNull().ShouldContain("not Azure Service Bus");
    }
}
