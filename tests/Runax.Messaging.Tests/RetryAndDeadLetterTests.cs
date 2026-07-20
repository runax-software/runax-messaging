using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class RetryAndDeadLetterTests
{
    private sealed record Work(int Id);

    private enum FailureMode
    {
        TransientThenSucceed,
        AlwaysThrow,
        Poison
    }

    private sealed class WorkState
    {
        public int Attempts;
        public FailureMode Mode { get; init; }
        public int FailuresBeforeSuccess { get; init; }
        public TaskCompletionSource<Work> Handled { get; } = new();
        public TaskCompletionSource<Work> DeadLettered { get; } = new();
    }

    private sealed class ConfigurableConsumer(WorkState state) : MessageConsumer<Work>
    {
        public override string Topic => "work";

        protected override ValueTask HandleAsync(Work message, CancellationToken cancellationToken)
        {
            var attempt = Interlocked.Increment(ref state.Attempts);

            switch (state.Mode)
            {
                case FailureMode.TransientThenSucceed when attempt <= state.FailuresBeforeSuccess:
                    throw new InvalidOperationException($"transient failure {attempt}");
                case FailureMode.AlwaysThrow:
                    throw new InvalidOperationException($"permanent failure {attempt}");
                case FailureMode.Poison:
                    throw new PoisonMessageException("cannot process");
                default:
                    state.Handled.TrySetResult(message);
                    return ValueTask.CompletedTask;
            }
        }
    }

    private sealed class WorkDeadLetterConsumer(WorkState state) : MessageConsumer<Work>
    {
        public override string Topic => "work.dead-letter";

        protected override ValueTask HandleAsync(Work message, CancellationToken cancellationToken)
        {
            state.DeadLettered.TrySetResult(message);
            return ValueTask.CompletedTask;
        }
    }

    private static IHost BuildHost(WorkState state, bool withDeadLetterConsumer)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(state);
        builder.Services.AddRunaxMessaging(m =>
        {
            m.AddInMemory()
                .AddConsumer<ConfigurableConsumer>()
                .WithRetry(o =>
                {
                    o.MaxAttempts = 3;
                    o.InitialDelay = TimeSpan.FromMilliseconds(1);
                    o.MaxDelay = TimeSpan.FromMilliseconds(5);
                });

            if (withDeadLetterConsumer)
                m.AddConsumer<WorkDeadLetterConsumer>();
        });

        return builder.Build();
    }

    [Fact]
    public async Task Transient_failures_are_retried_until_success()
    {
        var state = new WorkState { Mode = FailureMode.TransientThenSucceed, FailuresBeforeSuccess = 2 };
        using var host = BuildHost(state, withDeadLetterConsumer: false);
        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<IMessagePublisher>();
        await publisher.PublishAsync("work", new Work(1));

        var handled = await state.Handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        handled.Id.ShouldBe(1);
        state.Attempts.ShouldBe(3); // two failures then success

        await host.StopAsync();
    }

    [Fact]
    public async Task Exhausted_retries_dead_letter_the_message()
    {
        var state = new WorkState { Mode = FailureMode.AlwaysThrow };
        using var host = BuildHost(state, withDeadLetterConsumer: true);
        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<IMessagePublisher>();
        await publisher.PublishAsync("work", new Work(2));

        var deadLettered = await state.DeadLettered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        deadLettered.Id.ShouldBe(2);
        state.Attempts.ShouldBe(3); // capped at MaxAttempts

        await host.StopAsync();
    }

    [Fact]
    public async Task Poison_messages_skip_retries_and_dead_letter_immediately()
    {
        var state = new WorkState { Mode = FailureMode.Poison };
        using var host = BuildHost(state, withDeadLetterConsumer: true);
        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<IMessagePublisher>();
        await publisher.PublishAsync("work", new Work(3));

        var deadLettered = await state.DeadLettered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        deadLettered.Id.ShouldBe(3);
        state.Attempts.ShouldBe(1); // no retries for poison

        await host.StopAsync();
    }
}
