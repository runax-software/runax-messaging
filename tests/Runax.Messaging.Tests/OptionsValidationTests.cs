using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class OptionsValidationTests
{
    [Fact]
    public void Invalid_retry_options_throw_when_resolved()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory().WithRetry(o => o.MaxAttempts = 0));
        using var provider = services.BuildServiceProvider();

        Should.Throw<OptionsValidationException>(() => provider.GetRequiredService<RetryOptions>());
    }

    [Fact]
    public void Valid_retry_options_resolve()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory().WithRetry(o => o.MaxAttempts = 5));
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<RetryOptions>().MaxAttempts.ShouldBe(5);
    }

    [Fact]
    public void Retry_options_default_when_not_configured()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<RetryOptions>().MaxAttempts.ShouldBe(3);
    }
}
