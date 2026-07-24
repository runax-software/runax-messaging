using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.InMemory;
using Runax.Messaging.Transports.RabbitMq;

namespace Runax.Messaging.Transports.RabbitMq.Tests;

public class RabbitMqHealthCheckTests
{
    private static string HostName => Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "localhost";

    [Fact]
    public async Task Reports_unhealthy_when_the_transport_is_not_rabbitmq()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        services.AddHealthChecks().AddRabbitMqTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Unhealthy);
        report.Entries["rabbitmq"].Description.ShouldNotBeNull().ShouldContain("not RabbitMQ");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Reports_healthy_when_the_broker_is_reachable()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddRabbitMq(rabbit => rabbit.Configure(o => o.HostName = HostName)));
        services.AddHealthChecks().AddRabbitMqTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Healthy);
    }
}
