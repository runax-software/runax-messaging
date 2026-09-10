using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sqs.Tests;

public class SqsConfigBindingTests
{
    [Fact]
    public void Binds_config_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Sqs:Region"] = "eu-west-1",
                ["Sqs:MaxNumberOfMessages"] = "5",
                ["Sqs:VisibilityTimeoutSeconds"] = "60",
            })
            .Build();

        // The section-binding AddTransport overload uses the same Bind; assert the mapping directly.
        var config = new SqsConfig();
        configuration.GetSection("Sqs").Bind(config);

        config.Region.ShouldBe("eu-west-1");
        config.MaxNumberOfMessages.ShouldBe(5);
        config.VisibilityTimeoutSeconds.ShouldBe(60);

        // And the section overload registers a working transport.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport<SqsConfig>(configuration.GetSection("Sqs"))));
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)
            .ShouldBeOfType<SqsTransport>();
    }

    [Fact]
    public void Out_of_range_message_count_fails_validation_at_configuration_time()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var exception = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
                bus.AddTransport(new SqsConfig { MaxNumberOfMessages = 50 }))));

        exception.Message.ShouldContain("transport config is invalid");
    }
}
