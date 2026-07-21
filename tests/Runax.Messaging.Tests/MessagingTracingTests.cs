using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Diagnostics;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class MessagingTracingTests
{
    private sealed record Ping(string Value);

    private sealed class PingConsumer(TaskCompletionSource handled) : MessageConsumer<Ping>
    {
        public override string Topic => "trace-ping";

        protected override ValueTask HandleAsync(Ping message, CancellationToken cancellationToken)
        {
            handled.TrySetResult();
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task Trace_context_propagates_from_publish_to_consume()
    {
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessagingDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                lock (activities)
                    activities.Add(activity);
            },
        };
        ActivitySource.AddActivityListener(listener);

        var handled = new TaskCompletionSource();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(handled);
        builder.Services.AddRunaxMessaging(m => m.AddInMemory().AddConsumer<PingConsumer>());
        using var host = builder.Build();
        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<IMessagePublisher>();
        await publisher.PublishAsync("trace-ping", new Ping("hi"));

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The process span stops after the handler returns; wait for both spans to be recorded.
        Activity? publish = null;
        Activity? process = null;
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while ((publish is null || process is null) && DateTime.UtcNow < deadline)
        {
            lock (activities)
            {
                publish = activities.FirstOrDefault(a => a.OperationName == "trace-ping publish");
                process = activities.FirstOrDefault(a => a.OperationName == "trace-ping process");
            }

            if (publish is null || process is null)
                await Task.Delay(25);
        }

        await host.StopAsync();

        publish.ShouldNotBeNull();
        process.ShouldNotBeNull();
        publish!.Kind.ShouldBe(ActivityKind.Producer);
        process!.Kind.ShouldBe(ActivityKind.Consumer);
        publish.GetTagItem("messaging.system").ShouldBe("in-memory");
        process.TraceId.ShouldBe(publish.TraceId);
        process.ParentSpanId.ShouldBe(publish.SpanId);
    }
}
