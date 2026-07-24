using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Runax.Messaging.Transports.RabbitMq;

namespace Runax.Messaging.Transports.RabbitMq.Tests;

public class RabbitMqOptionsBindingTests
{
    [Fact]
    public void Binds_options_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RabbitMq:HostName"] = "broker.internal",
                ["RabbitMq:Port"] = "5671",
                ["RabbitMq:UseTls"] = "true",
                ["RabbitMq:Uri"] = "amqps://user:pass@broker.internal:5671/",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddRabbitMq(configuration.GetSection("RabbitMq")));
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<RabbitMqOptions>();
        options.HostName.ShouldBe("broker.internal");
        options.Port.ShouldBe(5671);
        options.UseTls.ShouldBeTrue();
        options.Uri.ShouldBe("amqps://user:pass@broker.internal:5671/");
    }

    [Fact]
    public void Invalid_port_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddRabbitMq(rabbit => rabbit.Configure(o => o.Port = 70000)));
        using var provider = services.BuildServiceProvider();

        Should.Throw<OptionsValidationException>(() => provider.GetRequiredService<RabbitMqOptions>());
    }
}
