using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class MultiTransportTests
{
    private sealed record Ping(string Id);

    private sealed class Collector
    {
        private readonly TaskCompletionSource _signal = new();
        private int _remaining = int.MaxValue;

        public ConcurrentBag<string> Handled { get; } = [];
        public Task Completed => _signal.Task;

        public void Expect(int count) => _remaining = count;

        public void Record(string id)
        {
            Handled.Add(id);
            if (Interlocked.Decrement(ref _remaining) == 0)
                _signal.TrySetResult();
        }
    }

    private sealed class PingConsumer(Collector collector) : MessageConsumer<Ping>
    {
        public override string Topic => "ping";

        protected override ValueTask HandleAsync(Ping message, CancellationToken cancellationToken)
        {
            collector.Record(message.Id);
            return ValueTask.CompletedTask;
        }
    }

    // In-process transport that can both receive injected messages and record what was published to it.
    private sealed class RecordingTransport(string systemName) : IMessagingTransport
    {
        private readonly Channel<(string Topic, string Json)> _channel = Channel.CreateUnbounded<(string, string)>();

        public string SystemName { get; } = systemName;

        public ConcurrentQueue<(string Topic, string Json)> Published { get; } = new();

        public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
        {
            Published.Enqueue((topic, envelopeJson));
            return _channel.Writer.WriteAsync((topic, envelopeJson), cancellationToken);
        }

        // Simulate a message arriving on this broker without recording it as a publish.
        public ValueTask DeliverAsync(string topic, string envelopeJson) =>
            _channel.Writer.WriteAsync((topic, envelopeJson));

        public async Task SubscribeAsync(
            string[] topics,
            Func<string, string, ValueTask<MessageDisposition>> onMessage,
            CancellationToken cancellationToken = default)
        {
            var wanted = new HashSet<string>(topics);
            try
            {
                await foreach (var (topic, json) in _channel.Reader.ReadAllAsync(cancellationToken))
                {
                    if (wanted.Contains(topic))
                        await onMessage(json, topic);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
        }
    }

    private static async Task<IHost> StartHostAsync(
        Collector collector,
        Action<MessagingConfigurator> configure,
        RecordingTransport a,
        RecordingTransport b)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(collector);
        builder.Services.AddRunaxMessaging(m =>
        {
            m.Services.AddSingleton<IMessagingTransport>(a);
            m.Services.AddSingleton<IMessagingTransport>(b);
            configure(m);
        });

        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    private static string Envelope(IHost host, Ping ping) =>
        host.Services.GetRequiredService<IMessageSerializer>().Serialize(ping, headers: null);

    [Fact]
    public async Task Untargeted_consumer_receives_its_topic_from_every_registered_transport()
    {
        var collector = new Collector();
        collector.Expect(2);
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        using var host = await StartHostAsync(collector, m => m.AddConsumer<PingConsumer>(), a, b);

        await a.DeliverAsync("ping", Envelope(host, new Ping("from-a")));
        await b.DeliverAsync("ping", Envelope(host, new Ping("from-b")));

        await collector.Completed.WaitAsync(TimeSpan.FromSeconds(5));
        collector.Handled.ShouldBe(["from-a", "from-b"], ignoreOrder: true);

        await host.StopAsync();
    }

    [Fact]
    public async Task Targeted_consumer_receives_only_from_its_transport()
    {
        var collector = new Collector();
        collector.Expect(1);
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        using var host = await StartHostAsync(collector, m => m.AddConsumer<PingConsumer>("broker-a"), a, b);

        await a.DeliverAsync("ping", Envelope(host, new Ping("from-a")));
        await b.DeliverAsync("ping", Envelope(host, new Ping("from-b")));

        await collector.Completed.WaitAsync(TimeSpan.FromSeconds(5));
        // Give the (unsubscribed) second transport a chance to leak a delivery before asserting the negative.
        await Task.Delay(200);

        collector.Handled.ShouldBe(["from-a"]);

        await host.StopAsync();
    }

    [Fact]
    public async Task Publishing_with_multiple_transports_and_no_selection_is_an_error()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.Services.AddSingleton<IMessagingTransport>(new RecordingTransport("broker-a"));
            m.Services.AddSingleton<IMessagingTransport>(new RecordingTransport("broker-b"));
        });
        await using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IMessagePublisher>();

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () => await publisher.PublishAsync("ping", new Ping("x")));
        ex.Message.ShouldContain("PublishTo");
    }

    [Fact]
    public async Task PublishTo_routes_publishes_to_the_selected_transport()
    {
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.Services.AddSingleton<IMessagingTransport>(a);
            m.Services.AddSingleton<IMessagingTransport>(b);
            m.PublishTo("broker-b");
        });
        await using var provider = services.BuildServiceProvider();

        await provider.GetRequiredService<IMessagePublisher>().PublishAsync("ping", new Ping("x"));

        b.Published.ShouldHaveSingleItem().Topic.ShouldBe("ping");
        a.Published.ShouldBeEmpty();
    }

    [Fact]
    public async Task Factory_For_publishes_to_the_named_transport()
    {
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.Services.AddSingleton<IMessagingTransport>(a);
            m.Services.AddSingleton<IMessagingTransport>(b);
        });
        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IMessagePublisherFactory>();

        await factory.ForTransport("broker-a").PublishAsync("ping", new Ping("x"));

        a.Published.ShouldHaveSingleItem().Topic.ShouldBe("ping");
        b.Published.ShouldBeEmpty();
    }

    [Fact]
    public async Task Factory_lets_a_single_event_go_to_two_transports_masstransit_style()
    {
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.Services.AddSingleton<IMessagingTransport>(a);
            m.Services.AddSingleton<IMessagingTransport>(b);
        });
        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IMessagePublisherFactory>();

        var evt = new Ping("both");
        await factory.ForTransport("broker-a").PublishAsync("ping", evt);
        await factory.ForTransport("broker-b").PublishAsync("ping", evt);

        a.Published.ShouldHaveSingleItem().Topic.ShouldBe("ping");
        b.Published.ShouldHaveSingleItem().Topic.ShouldBe("ping");
    }

    [Fact]
    public async Task Factory_caches_a_publisher_per_transport_name()
    {
        var a = new RecordingTransport("broker-a");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.Services.AddSingleton<IMessagingTransport>(a));
        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IMessagePublisherFactory>();

        factory.ForTransport("broker-a").ShouldBeSameAs(factory.ForTransport("broker-a"));
    }

    [Fact]
    public async Task Factory_For_with_an_unknown_transport_name_throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
            m.Services.AddSingleton<IMessagingTransport>(new RecordingTransport("broker-a")));
        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IMessagePublisherFactory>();

        var ex = Should.Throw<InvalidOperationException>(() => factory.ForTransport("nope"));
        ex.Message.ShouldContain("nope");
        ex.Message.ShouldContain("broker-a");
    }

    [Fact]
    public async Task A_consumer_registered_in_a_transport_block_is_subscribed()
    {
        var collector = new Collector();
        collector.Expect(1);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(collector);
        builder.Services.AddRunaxMessaging(m => m.AddInMemory(mem => mem.AddConsumer<PingConsumer>()));
        using var host = builder.Build();
        await host.StartAsync();

        await host.Services.GetRequiredService<IMessagePublisher>().PublishAsync("ping", new Ping("scoped"));

        await collector.Completed.WaitAsync(TimeSpan.FromSeconds(5));
        collector.Handled.ShouldContain("scoped");

        await host.StopAsync();
    }

    [Fact]
    public async Task The_same_consumer_bound_to_two_brokers_receives_from_both_as_one_instance()
    {
        var collector = new Collector();
        collector.Expect(2);
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        using var host = await StartHostAsync(collector, m =>
        {
            new TransportBuilder(m.Services, "broker-a").AddConsumer<PingConsumer>();
            new TransportBuilder(m.Services, "broker-b").AddConsumer<PingConsumer>();
        }, a, b);

        // Registered against two brokers, but a single instance handles both.
        host.Services.GetServices<PingConsumer>().Count().ShouldBe(1);

        await a.DeliverAsync("ping", Envelope(host, new Ping("from-a")));
        await b.DeliverAsync("ping", Envelope(host, new Ping("from-b")));

        await collector.Completed.WaitAsync(TimeSpan.FromSeconds(5));
        collector.Handled.ShouldBe(["from-a", "from-b"], ignoreOrder: true);

        await host.StopAsync();
    }
}
