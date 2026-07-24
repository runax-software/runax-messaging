using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.InMemory;
using Runax.Messaging.Transports.Kafka;

namespace Runax.Messaging.Transports.Kafka.Tests;

public class KafkaHealthCheckTests
{
    private static string BootstrapServers =>
        Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";

    [Fact]
    public async Task Reports_unhealthy_when_the_transport_is_not_kafka()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        services.AddHealthChecks().AddKafkaTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Unhealthy);
        report.Entries["kafka"].Description.ShouldNotBeNull().ShouldContain("not Kafka");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Reports_healthy_when_the_cluster_is_reachable()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddKafka(kafka => kafka.Configure(o => o.BootstrapServers = BootstrapServers)));
        services.AddHealthChecks().AddKafkaTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Healthy);
    }
}
