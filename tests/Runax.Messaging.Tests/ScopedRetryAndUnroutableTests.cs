using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class ScopedRetryAndUnroutableTests
{
    // A second, do-nothing transport so we can prove a setting on one broker leaves the other untouched.
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

    [Fact]
    public void Scoped_WithRetry_applies_only_to_that_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddInMemory(inMemory => inMemory.WithRetry(o => o.MaxAttempts = 7));
            m.Services.AddSingleton<IMessagingTransport>(new FakeTransport("other"));
        });
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();
        var inMemoryName = provider.GetServices<IMessagingTransport>()
            .First(t => t.SystemName != "other").SystemName;

        // The in-memory broker got its scoped policy...
        retry.For(inMemoryName).MaxAttempts.ShouldBe(7);
        // ...but the other broker falls back to the built-in default.
        retry.For("other").MaxAttempts.ShouldBe(3);
    }

    [Fact]
    public void Scoped_WithRetry_overrides_the_global_policy_for_that_transport_only()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddInMemory(inMemory => inMemory.WithRetry(o => o.MaxAttempts = 7));
            m.Services.AddSingleton<IMessagingTransport>(new FakeTransport("other"));
            m.WithRetry(o => o.MaxAttempts = 5); // global default
        });
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();
        var inMemoryName = provider.GetServices<IMessagingTransport>()
            .First(t => t.SystemName != "other").SystemName;

        retry.For(inMemoryName).MaxAttempts.ShouldBe(7); // scoped wins
        retry.For("other").MaxAttempts.ShouldBe(5);      // global applies
    }

    [Fact]
    public void Global_only_WithRetry_still_applies_to_every_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddInMemory();
            m.Services.AddSingleton<IMessagingTransport>(new FakeTransport("other"));
            m.WithRetry(o => o.MaxAttempts = 9);
        });
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();

        retry.For("in-memory").MaxAttempts.ShouldBe(9);
        retry.For("other").MaxAttempts.ShouldBe(9);
    }

    [Fact]
    public void Default_WithRetry_uses_the_built_in_defaults()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        using var provider = services.BuildServiceProvider();

        var retry = provider.GetRequiredService<IRetryOptionsProvider>();

        retry.For("in-memory").MaxAttempts.ShouldBe(3);
    }

    [Fact]
    public void Scoped_OnUnroutableMessage_strategy_applies_only_to_that_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddInMemory(inMemory => inMemory.OnUnroutableMessage(UnroutableStrategy.Discard));
            m.Services.AddSingleton<IMessagingTransport>(new FakeTransport("other"));
        });
        using var provider = services.BuildServiceProvider();

        var handlers = provider.GetRequiredService<IUnroutableMessageHandlerProvider>();
        var inMemoryName = provider.GetServices<IMessagingTransport>()
            .First(t => t.SystemName != "other").SystemName;

        // The in-memory broker discards (acknowledges)...
        Handle(handlers.For(inMemoryName)).ShouldBe(MessageDisposition.Acknowledge);
        // ...while the other broker keeps the built-in dead-letter default.
        Handle(handlers.For("other")).ShouldBe(MessageDisposition.DeadLetter);
    }

    [Fact]
    public void Scoped_OnUnroutableMessage_handler_applies_only_to_that_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddInMemory(inMemory => inMemory.OnUnroutableMessage<QuarantineHandler>());
            m.Services.AddSingleton<IMessagingTransport>(new FakeTransport("other"));
        });
        using var provider = services.BuildServiceProvider();

        var handlers = provider.GetRequiredService<IUnroutableMessageHandlerProvider>();
        var inMemoryName = provider.GetServices<IMessagingTransport>()
            .First(t => t.SystemName != "other").SystemName;

        handlers.For(inMemoryName).ShouldBeOfType<QuarantineHandler>();
        Handle(handlers.For("other")).ShouldBe(MessageDisposition.DeadLetter);
    }

    [Fact]
    public void Scoped_OnUnroutableMessage_overrides_the_global_strategy_for_that_transport_only()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddInMemory(inMemory => inMemory.OnUnroutableMessage(UnroutableStrategy.Discard));
            m.Services.AddSingleton<IMessagingTransport>(new FakeTransport("other"));
            m.OnUnroutableMessage(UnroutableStrategy.Requeue); // global default
        });
        using var provider = services.BuildServiceProvider();

        var handlers = provider.GetRequiredService<IUnroutableMessageHandlerProvider>();
        var inMemoryName = provider.GetServices<IMessagingTransport>()
            .First(t => t.SystemName != "other").SystemName;

        Handle(handlers.For(inMemoryName)).ShouldBe(MessageDisposition.Acknowledge); // scoped Discard wins
        Handle(handlers.For("other")).ShouldBe(MessageDisposition.Requeue);          // global Requeue applies
    }

    [Fact]
    public void Global_only_OnUnroutableMessage_still_applies_to_every_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddInMemory();
            m.Services.AddSingleton<IMessagingTransport>(new FakeTransport("other"));
            m.OnUnroutableMessage(UnroutableStrategy.Discard);
        });
        using var provider = services.BuildServiceProvider();

        var handlers = provider.GetRequiredService<IUnroutableMessageHandlerProvider>();

        Handle(handlers.For("in-memory")).ShouldBe(MessageDisposition.Acknowledge);
        Handle(handlers.For("other")).ShouldBe(MessageDisposition.Acknowledge);
    }

    [Fact]
    public void Default_OnUnroutableMessage_dead_letters()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        using var provider = services.BuildServiceProvider();

        var handlers = provider.GetRequiredService<IUnroutableMessageHandlerProvider>();

        Handle(handlers.For("in-memory")).ShouldBe(MessageDisposition.DeadLetter);
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
