using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sqs.Tests;

public class SqsHealthCheckTests
{
    private static string ServiceUrl => Environment.GetEnvironmentVariable("AWS_SERVICE_URL") ?? "http://localhost:4566";
    private static string Region => Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1";

    [Fact]
    public void Health_check_is_auto_registered_for_the_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new SqsConfig())));
        using var provider = services.BuildServiceProvider();

        var registrations = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        registrations.ShouldContain(r => r.Name == $"runax:{BusNames.Default}");
    }

    [Fact]
    public void Health_check_is_not_registered_when_disabled()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new SqsConfig { RegisterHealthCheck = false })));
        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations.ShouldBeEmpty();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Reports_healthy_when_the_endpoint_is_reachable()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new SqsConfig
        {
            Region = Region,
            ServiceUrl = ServiceUrl,
            AccessKey = "test",
            SecretKey = "test",
        })));
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(r => r.Name == $"runax:{BusNames.Default}");

        report.Status.ShouldBe(HealthStatus.Healthy);
        report.Entries[$"runax:{BusNames.Default}"].Status.ShouldBe(HealthStatus.Healthy);
    }
}
