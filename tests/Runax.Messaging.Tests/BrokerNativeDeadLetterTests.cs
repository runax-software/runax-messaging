using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Tests;

public class BrokerNativeDeadLetterTests
{
    private sealed record Work(int Id);

    private sealed class FailingConsumer : MessageConsumer<Work>
    {
        public override string Topic => "work";

        protected override ValueTask HandleAsync(Work message, CancellationToken cancellationToken)
            => throw new InvalidOperationException("always fails");
    }

    private sealed class PoisonConsumer : MessageConsumer<Work>
    {
        public override string Topic => "work";

        protected override ValueTask HandleAsync(Work message, CancellationToken cancellationToken)
            => throw new PoisonMessageException("cannot process");
    }

    /// <summary>
    /// Captures the callback the hosted service registers so a message can be delivered on demand,
    /// and records every publish so framework-managed dead-letter republishes are observable.
    /// </summary>
    private sealed class CapturingTransport : IMessagingTransport
    {
        private Func<string, string, ValueTask<MessageDisposition>>? _onMessage;
        private readonly TaskCompletionSource _subscribed = new();

        public string SystemName => "capturing";

        public List<(string Topic, string Envelope)> Published { get; } = [];

        public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
        {
            Published.Add((topic, envelopeJson));
            return ValueTask.CompletedTask;
        }

        public Task SubscribeAsync(
            string[] topics,
            Func<string, string, ValueTask<MessageDisposition>> onMessage,
            CancellationToken cancellationToken = default)
        {
            _onMessage = onMessage;
            _subscribed.TrySetResult();
            return Task.Delay(Timeout.Infinite, cancellationToken);
        }

        public Task WaitUntilSubscribedAsync() => _subscribed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        public ValueTask<MessageDisposition> DeliverAsync(string topic, string envelope) => _onMessage!(envelope, topic);
    }

    private static async Task<(IHost Host, CapturingTransport Transport)> StartHostAsync<TConsumer>(
        DeadLetterStrategy strategy)
        where TConsumer : class
    {
        var transport = new CapturingTransport();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IMessagingTransport>(transport);
        builder.Services.AddRunaxMessaging(m =>
        {
            m.AddConsumer<TConsumer>()
                .WithRetry(o =>
                {
                    o.MaxAttempts = 2;
                    o.InitialDelay = TimeSpan.FromMilliseconds(1);
                    o.MaxDelay = TimeSpan.FromMilliseconds(2);
                    o.Strategy = strategy;
                });
        });

        var host = builder.Build();
        await host.StartAsync();
        await transport.WaitUntilSubscribedAsync();
        return (host, transport);
    }

    private static async Task<string> PublishAndCaptureEnvelopeAsync(IHost host, CapturingTransport transport)
    {
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();
        await publisher.PublishAsync("work", new Work(1));

        var envelope = transport.Published.Single(p => p.Topic == "work").Envelope;
        transport.Published.Clear();
        return envelope;
    }

    [Fact]
    public async Task BrokerNative_exhausted_message_is_rejected_without_republish()
    {
        var (host, transport) = await StartHostAsync<FailingConsumer>(DeadLetterStrategy.BrokerNative);
        var envelope = await PublishAndCaptureEnvelopeAsync(host, transport);

        var disposition = await transport.DeliverAsync("work", envelope);

        disposition.ShouldBe(MessageDisposition.DeadLetter);
        transport.Published.ShouldBeEmpty(); // broker handles dead-lettering, not the framework

        await host.StopAsync();
    }

    [Fact]
    public async Task BrokerNative_poison_message_is_rejected_immediately()
    {
        var (host, transport) = await StartHostAsync<PoisonConsumer>(DeadLetterStrategy.BrokerNative);
        var envelope = await PublishAndCaptureEnvelopeAsync(host, transport);

        var disposition = await transport.DeliverAsync("work", envelope);

        disposition.ShouldBe(MessageDisposition.DeadLetter);
        transport.Published.ShouldBeEmpty();

        await host.StopAsync();
    }

    [Fact]
    public async Task FrameworkManaged_exhausted_message_is_acknowledged_after_republish()
    {
        var (host, transport) = await StartHostAsync<FailingConsumer>(DeadLetterStrategy.FrameworkManaged);
        var envelope = await PublishAndCaptureEnvelopeAsync(host, transport);

        var disposition = await transport.DeliverAsync("work", envelope);

        disposition.ShouldBe(MessageDisposition.Acknowledge);
        transport.Published.ShouldContain(p => p.Topic == "work.dead-letter");

        await host.StopAsync();
    }
}
