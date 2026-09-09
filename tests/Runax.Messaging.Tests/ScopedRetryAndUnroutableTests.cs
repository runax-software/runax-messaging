using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class ScopedRetryAndUnroutableTests
{
    // A second, do-nothing transport so we can prove a setting on one bus leaves the other untouched.
    private sealed class FakeTransport(string systemName) : IMessagingTransport
    {
        public string SystemName { get; } = systemName;

        public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default) =>
            ValueTask.CompletedTask;

        public Task SubscribeAsync(
            string[] topics,
            Func<string, string, ValueTask<MessageDisposition>> onMessage,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    private sealed class QuarantineHandler : IUnroutableMessageHandler
    {
        public ValueTask<MessageDisposition> HandleAsync(UnroutableMessage message, CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(MessageDisposition.Requeue);
    }

    private static void AddOtherBus(MessagingConfigurator m, Action<BusBuilder>? configure = null) =>
        m.AddBus("other", bus =>
        {
            bus.AddTransport(new FakeTransportConfig(new FakeTransport("other")));
            configure?.Invoke(bus);
        });

    [Fact]
    public void Bus_WithRetry_applies_only_to_that_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus(bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.WithRetry(o => o.MaxAttempts = 7);
            });
            AddOtherBus(m);
        });
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();

        // The default bus got its policy...
        retry.For(BusNames.Default, "any").MaxAttempts.ShouldBe(7);
        // ...but the other bus falls back to the built-in default.
        retry.For("other", "any").MaxAttempts.ShouldBe(3);
    }

    [Fact]
    public void Each_bus_keeps_its_own_WithRetry_policy()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus(bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.WithRetry(o => o.MaxAttempts = 7);
            });
            AddOtherBus(m, bus => bus.WithRetry(o => o.MaxAttempts = 5));
        });
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();

        retry.For(BusNames.Default, "any").MaxAttempts.ShouldBe(7);
        retry.For("other", "any").MaxAttempts.ShouldBe(5);
    }

    [Fact]
    public void Bus_WithRetry_applies_to_every_topic_on_that_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.WithRetry(o => o.MaxAttempts = 9);
        }));
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();

        retry.For(BusNames.Default, "payments").MaxAttempts.ShouldBe(9);
        retry.For(BusNames.Default, "telemetry").MaxAttempts.ShouldBe(9);
    }

    [Fact]
    public void Default_WithRetry_uses_the_built_in_defaults()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new InMemoryConfig())));
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();

        retry.For(BusNames.Default, "any").MaxAttempts.ShouldBe(3);
    }

    [Fact]
    public void WithRetryForTopic_applies_only_to_that_topic()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.WithRetryForTopic("payments", o => o.MaxAttempts = 10);
        }));
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();

        // The "payments" topic gets the per-topic policy...
        retry.For(BusNames.Default, "payments").MaxAttempts.ShouldBe(10);
        // ...while every other topic keeps the built-in default.
        retry.For(BusNames.Default, "telemetry").MaxAttempts.ShouldBe(3);
    }

    [Fact]
    public void Topic_retry_policy_wins_over_bus_policy_for_the_same_topic()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus(bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.WithRetry(o => o.MaxAttempts = 7);                       // bus-wide: any topic
                bus.WithRetryForTopic("payments", o => o.MaxAttempts = 10);  // bus + topic
            });
            AddOtherBus(m, bus => bus.WithRetryForTopic("payments", o => o.MaxAttempts = 99));
        });
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();

        // Bus+topic is the most specific scope, so it wins over the bus-wide policy.
        retry.For(BusNames.Default, "payments").MaxAttempts.ShouldBe(10);
        // A different topic on the same bus still gets the bus-wide policy.
        retry.For(BusNames.Default, "telemetry").MaxAttempts.ShouldBe(7);
        // The other bus's per-topic policy is independent of the default bus.
        retry.For("other", "payments").MaxAttempts.ShouldBe(99);
    }

    [Fact]
    public void Bus_OnUnroutableMessage_strategy_applies_only_to_that_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus(bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.OnUnroutableMessage(UnroutableStrategy.Discard);
            });
            AddOtherBus(m);
        });
        using var provider = services.BuildServiceProvider();

        var handlers = provider.GetRequiredService<IUnroutableMessageHandlerProvider>();

        // The default bus discards (acknowledges)...
        Handle(handlers.For(BusNames.Default)).ShouldBe(MessageDisposition.Acknowledge);
        // ...while the other bus keeps the built-in dead-letter default.
        Handle(handlers.For("other")).ShouldBe(MessageDisposition.DeadLetter);
    }

    [Fact]
    public void Bus_OnUnroutableMessage_handler_applies_only_to_that_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus(bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.OnUnroutableMessage<QuarantineHandler>();
            });
            AddOtherBus(m);
        });
        using var provider = services.BuildServiceProvider();

        var handlers = provider.GetRequiredService<IUnroutableMessageHandlerProvider>();

        handlers.For(BusNames.Default).ShouldBeOfType<QuarantineHandler>();
        Handle(handlers.For("other")).ShouldBe(MessageDisposition.DeadLetter);
    }

    [Fact]
    public void Each_bus_keeps_its_own_OnUnroutableMessage_strategy()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus(bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.OnUnroutableMessage(UnroutableStrategy.Discard);
            });
            AddOtherBus(m, bus => bus.OnUnroutableMessage(UnroutableStrategy.Requeue));
        });
        using var provider = services.BuildServiceProvider();

        var handlers = provider.GetRequiredService<IUnroutableMessageHandlerProvider>();

        Handle(handlers.For(BusNames.Default)).ShouldBe(MessageDisposition.Acknowledge); // Discard
        Handle(handlers.For("other")).ShouldBe(MessageDisposition.Requeue);              // Requeue
    }

    [Fact]
    public void Default_OnUnroutableMessage_dead_letters()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new InMemoryConfig())));
        using var provider = services.BuildServiceProvider();

        var handlers = provider.GetRequiredService<IUnroutableMessageHandlerProvider>();

        Handle(handlers.For(BusNames.Default)).ShouldBe(MessageDisposition.DeadLetter);
    }

    private static MessageDisposition Handle(IUnroutableMessageHandler handler) =>
        handler.HandleAsync(new UnroutableMessage
        {
            Topic = "t",
            ContractName = "c",
            ContractVersion = 1,
            Body = "{}",
            Headers = new Dictionary<string, string>(),
            TransportSystemName = "x",
        }).AsTask().GetAwaiter().GetResult();
}
