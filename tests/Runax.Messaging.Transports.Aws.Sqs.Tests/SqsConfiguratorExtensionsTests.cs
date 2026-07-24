using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Aws.Sqs;

namespace Runax.Messaging.Transports.Aws.Sqs.Tests;

public class SqsConfiguratorExtensionsTests
{
    [Fact]
    public void AddSqs_registers_the_transport_and_applies_options()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddSqs(sqs => sqs.Configure(o =>
        {
            o.Region = "eu-west-1";
            o.ServiceUrl = "http://localhost:4566";
            o.AccessKey = "test";
            o.SecretKey = "test";
        })));

        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<SqsOptions>();
        options.Region.ShouldBe("eu-west-1");
        options.ServiceUrl.ShouldBe("http://localhost:4566");

        provider.GetRequiredService<IMessagingTransport>().ShouldBeOfType<SqsTransport>();
    }

    [Fact]
    public void AddSqs_returns_the_same_configurator()
    {
        var services = new ServiceCollection();
        var configurator = new MessagingConfigurator(services);

        var result = configurator.AddSqs(sqs => sqs.Configure(_ => { }));

        result.ShouldBeSameAs(configurator);
    }
}
