using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class OptionsValidationTests
{
    [Fact]
    public async Task Invalid_retry_options_throw_on_host_start()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.WithRetry(o => o.MaxAttempts = 0);
        }));
        using var host = builder.Build();

        await Should.ThrowAsync<OptionsValidationException>(host.StartAsync());
    }

    [Fact]
    public async Task Invalid_topic_retry_options_throw_on_host_start()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.WithRetryForTopic("orders", o => o.BackoffFactor = 0.5);
        }));
        using var host = builder.Build();

        var ex = await Should.ThrowAsync<OptionsValidationException>(host.StartAsync());
        ex.Message.ShouldContain(nameof(RetryOptions.BackoffFactor));
    }

    [Fact]
    public void Valid_retry_options_resolve()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.WithRetry(o => o.MaxAttempts = 5);
        }));
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IRetryOptionsProvider>()
            .For(BusNames.Default, "any").MaxAttempts.ShouldBe(5);
    }

    [Fact]
    public void Retry_options_default_when_not_configured()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new InMemoryConfig())));
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<RetryOptions>().MaxAttempts.ShouldBe(3);
    }
}
