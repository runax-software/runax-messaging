using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Diagnostics;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class MessagingMetricsTests
{
    private sealed record Ping(int Id);

    // Distinct topics keep each test's measurements isolated from other tests that share the
    // process-static meter when the runner executes classes in parallel.
    private const string SuccessTopic = "metrics-success";
    private const string FailureTopic = "metrics-failed";

    private sealed class SuccessConsumer(TaskCompletionSource handled) : MessageConsumer<Ping>
    {
        public override string Topic => SuccessTopic;

        protected override ValueTask HandleAsync(Ping message, CancellationToken cancellationToken)
        {
            handled.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class FailingConsumer(TaskCompletionSource failed) : MessageConsumer<Ping>
    {
        public override string Topic => FailureTopic;

        protected override ValueTask HandleAsync(Ping message, CancellationToken cancellationToken)
        {
            failed.TrySetResult();
            throw new InvalidOperationException("boom");
        }
    }

    private static long CountFor(MetricCollector<long> collector, string topic) =>
        collector.GetMeasurementSnapshot()
            .Where(m => m.Tags.TryGetValue("messaging.destination.name", out var t) && (string?)t == topic)
            .Sum(m => m.Value);

    [Fact]
    public async Task Published_consumed_and_duration_are_recorded_on_success()
    {
        using var published = new MetricCollector<long>(null, MessagingDiagnostics.MeterName, "runax.messaging.published");
        using var consumed = new MetricCollector<long>(null, MessagingDiagnostics.MeterName, "runax.messaging.consumed");
        using var duration = new MetricCollector<double>(null, MessagingDiagnostics.MeterName, "runax.messaging.processing.duration");

        var handled = new TaskCompletionSource();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(handled);
        builder.Services.AddRunaxMessaging(m => m.AddInMemory().AddConsumer<SuccessConsumer>());
        using var host = builder.Build();
        await host.StartAsync();

        await host.Services.GetRequiredService<IMessagePublisher>().PublishAsync(SuccessTopic, new Ping(1));
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await consumed.WaitForMeasurementsAsync(1).WaitAsync(TimeSpan.FromSeconds(5));
        await host.StopAsync();

        CountFor(published, SuccessTopic).ShouldBe(1);
        CountFor(consumed, SuccessTopic).ShouldBe(1);
        duration.GetMeasurementSnapshot()
            .Any(m => m.Tags.TryGetValue("messaging.destination.name", out var t) && (string?)t == SuccessTopic)
            .ShouldBeTrue();
    }

    [Fact]
    public async Task Failed_is_recorded_when_a_consumer_gives_up()
    {
        using var failedCounter = new MetricCollector<long>(null, MessagingDiagnostics.MeterName, "runax.messaging.failed");
        using var consumed = new MetricCollector<long>(null, MessagingDiagnostics.MeterName, "runax.messaging.consumed");

        var failed = new TaskCompletionSource();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(failed);
        builder.Services.AddRunaxMessaging(m =>
        {
            m.AddInMemory()
                .AddConsumer<FailingConsumer>()
                .WithRetry(o =>
                {
                    o.MaxAttempts = 1;
                    o.InitialDelay = TimeSpan.FromMilliseconds(1);
                });
        });
        using var host = builder.Build();
        await host.StartAsync();

        await host.Services.GetRequiredService<IMessagePublisher>().PublishAsync(FailureTopic, new Ping(2));
        await failed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await failedCounter.WaitForMeasurementsAsync(1).WaitAsync(TimeSpan.FromSeconds(5));
        await host.StopAsync();

        CountFor(failedCounter, FailureTopic).ShouldBe(1);
        CountFor(consumed, FailureTopic).ShouldBe(0);
    }
}
