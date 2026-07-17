using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Tests;

public class ServiceCollectionExtensionsTests
{
    private sealed record Ping(string Value);

    private sealed class PingConsumer : MessageConsumer<Ping>
    {
        public override string Topic => "ping";
        protected override ValueTask HandleAsync(Ping message, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }

    [Fact]
    public void AddRunaxMessaging_registers_the_publisher_and_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddInMemory());

        using var provider = services.BuildServiceProvider();
        provider.GetService<IMessagePublisher>().ShouldNotBeNull();
        provider.GetService<IMessagingTransport>().ShouldNotBeNull();
    }

    [Fact]
    public void AddRunaxMessaging_without_consumers_registers_no_hosted_service()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddInMemory());

        services.ShouldNotContain(d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void AddConsumer_registers_the_consumer_and_the_hosted_dispatcher()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddRunaxMessaging(m => m.AddInMemory().AddConsumer<PingConsumer>());

        services.ShouldContain(d => d.ServiceType == typeof(PingConsumer));
        services.ShouldContain(d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void AddRunaxMessaging_returns_the_same_service_collection()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var result = services.AddRunaxMessaging(m => m.AddInMemory());

        result.ShouldBeSameAs(services);
    }
}
