using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Runax.Messaging.Transports.Aws.Sqs;

namespace Runax.Messaging.Transports.Aws.Sqs.Tests;

public class SqsOptionsBindingTests
{
    [Fact]
    public void Binds_options_from_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Sqs:Region"] = "eu-west-1",
                ["Sqs:MaxNumberOfMessages"] = "5",
                ["Sqs:VisibilityTimeoutSeconds"] = "60",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSqs(configuration.GetSection("Sqs")));
        using var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<SqsOptions>();
        options.Region.ShouldBe("eu-west-1");
        options.MaxNumberOfMessages.ShouldBe(5);
        options.VisibilityTimeoutSeconds.ShouldBe(60);
    }

    [Fact]
    public void Out_of_range_message_count_fails_validation()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSqs(__tb => __tb.Configure(o => o.MaxNumberOfMessages = 50)));
        using var provider = services.BuildServiceProvider();

        Should.Throw<OptionsValidationException>(() => provider.GetRequiredService<SqsOptions>());
    }
}
