using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging;
using Runax.Messaging.Abstractions;
using Runax.Messaging.TestKit;

namespace Runax.Messaging.TestKit.Tests;

public class MessagingTestHarnessTests
{
    private const string Topic = "orders.placed";

    private sealed record OrderPlaced(int Id);

    // A dependency the consumer under test needs — the harness injects it so we can observe handling.
    private sealed class OrderStore
    {
        private readonly List<int> _handled = [];
        private readonly Lock _gate = new();

        public IReadOnlyList<int> Handled
        {
            get
            {
                lock (_gate)
                    return _handled.ToArray();
            }
        }

        public void Add(int id)
        {
            lock (_gate)
                _handled.Add(id);
        }
    }

    private sealed class OrderPlacedConsumer(OrderStore store) : MessageConsumer<OrderPlaced>
    {
        public override string Topic => MessagingTestHarnessTests.Topic;

        protected override ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken)
        {
            store.Add(message.Id);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class AlwaysFailsConsumer : MessageConsumer<OrderPlaced>
    {
        public static int Attempts;

        public override string Topic => MessagingTestHarnessTests.Topic;

        protected override ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Attempts);
            throw new InvalidOperationException("boom");
        }
    }

    private sealed class TransientThenSucceedsConsumer(OrderStore store) : MessageConsumer<OrderPlaced>
    {
        public static int Attempts;

        public override string Topic => MessagingTestHarnessTests.Topic;

        protected override ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref Attempts) < 3)
                throw new InvalidOperationException("transient");

            store.Add(message.Id);
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task Published_message_is_delivered_to_the_registered_consumer()
    {
        var store = new OrderStore();
        await using var harness = await MessagingTestHarness.Create()
            .AddService(store)
            .AddConsumer<OrderPlacedConsumer>()
            .StartAsync();

        await harness.PublishAsync(Topic, new OrderPlaced(42));

        var recorded = await harness.WaitForConsumedAsync(Topic);

        recorded.Topic.ShouldBe(Topic);
        recorded.Disposition.ShouldBe(MessageDisposition.Acknowledge);
        store.Handled.ShouldBe([42]);
    }

    [Fact]
    public async Task WaitForConsumed_returns_the_deserialized_payload()
    {
        var store = new OrderStore();
        await using var harness = await MessagingTestHarness.Create()
            .AddService(store)
            .AddConsumer<OrderPlacedConsumer>()
            .StartAsync();

        await harness.PublishAsync(Topic, new OrderPlaced(7));

        var order = await harness.WaitForConsumedAsync<OrderPlaced>(Topic);

        order.Id.ShouldBe(7);
    }

    [Fact]
    public async Task Injected_dependency_is_used_by_the_consumer()
    {
        // The dependency is resolvable both by the consumer and from the running harness.
        var store = new OrderStore();
        await using var harness = await MessagingTestHarness.Create()
            .AddService(store)
            .AddConsumer<OrderPlacedConsumer>()
            .StartAsync();

        await harness.PublishAsync(Topic, new OrderPlaced(1));
        await harness.PublishAsync(Topic, new OrderPlaced(2));

        await harness.WaitForConsumedAsync(Topic);

        harness.Services.GetRequiredService<OrderStore>().ShouldBeSameAs(store);
    }

    [Fact]
    public async Task Exhausted_retries_are_dead_lettered()
    {
        AlwaysFailsConsumer.Attempts = 0;
        await using var harness = await MessagingTestHarness.Create()
            .AddConsumer<AlwaysFailsConsumer>()
            .ConfigureMessaging(m => m.WithRetry(o =>
            {
                o.MaxAttempts = 2;
                o.InitialDelay = TimeSpan.FromMilliseconds(1);
                o.MaxDelay = TimeSpan.FromMilliseconds(2);
            }))
            .StartAsync();

        await harness.PublishAsync(Topic, new OrderPlaced(99));

        var deadLettered = await harness.WaitForDeadLetterAsync(Topic);

        deadLettered.Topic.ShouldBe(Topic + ".dead-letter");
        deadLettered.As<OrderPlaced>()!.Id.ShouldBe(99);
        AlwaysFailsConsumer.Attempts.ShouldBe(2);
    }

    [Fact]
    public async Task Transient_failures_are_retried_until_success()
    {
        TransientThenSucceedsConsumer.Attempts = 0;
        var store = new OrderStore();
        await using var harness = await MessagingTestHarness.Create()
            .AddService(store)
            .AddConsumer<TransientThenSucceedsConsumer>()
            .ConfigureMessaging(m => m.WithRetry(o =>
            {
                o.MaxAttempts = 5;
                o.InitialDelay = TimeSpan.FromMilliseconds(1);
                o.MaxDelay = TimeSpan.FromMilliseconds(2);
            }))
            .StartAsync();

        await harness.PublishAsync(Topic, new OrderPlaced(5));

        var recorded = await harness.WaitForConsumedAsync(Topic);

        recorded.As<OrderPlaced>()!.Id.ShouldBe(5);
        store.Handled.ShouldBe([5]);
        TransientThenSucceedsConsumer.Attempts.ShouldBe(3); // two transient failures, then success
        harness.ConsumedCount(Topic).ShouldBe(1);
    }

    [Fact]
    public async Task Delivered_exposes_every_observed_message()
    {
        var store = new OrderStore();
        await using var harness = await MessagingTestHarness.Create()
            .AddService(store)
            .AddConsumer<OrderPlacedConsumer>()
            .StartAsync();

        await harness.PublishAsync(Topic, new OrderPlaced(10));
        await harness.WaitForConsumedAsync(Topic);

        harness.Delivered.ShouldNotBeEmpty();
        harness.Delivered.ShouldContain(m => m.Topic == Topic && m.As<OrderPlaced>()!.Id == 10);
    }
}
