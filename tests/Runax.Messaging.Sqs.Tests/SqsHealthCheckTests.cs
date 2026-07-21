using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.InMemory;
using Runax.Messaging.Sqs;

namespace Runax.Messaging.Sqs.Tests;

public class SqsHealthCheckTests
{
    private static string ServiceUrl => Environment.GetEnvironmentVariable("AWS_SERVICE_URL") ?? "http://localhost:4566";
    private static string Region => Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1";

    [Fact]
    public async Task Reports_unhealthy_when_the_transport_is_not_sqs()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        services.AddHealthChecks().AddSqsTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Unhealthy);
        report.Entries["sqs"].Description.ShouldContain("not SQS");
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Reports_healthy_when_the_endpoint_is_reachable()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSqs(o =>
        {
            o.Region = Region;
            o.ServiceUrl = ServiceUrl;
            o.AccessKey = "test";
            o.SecretKey = "test";
        }));
        services.AddHealthChecks().AddSqsTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Healthy);
    }
}
