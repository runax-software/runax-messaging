using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class MultiBusTests
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

    // Two named buses over two independent brokers; each test decides what to register per bus.
    private static async Task<IHost> StartHostAsync(
        Collector collector,
        RecordingTransport a,
        RecordingTransport b,
        Action<BusBuilder> busA,
        Action<BusBuilder> busB)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(collector);
        builder.Services.AddRunaxMessaging(m =>
        {
            m.AddBus("broker-a", bus =>
            {
                bus.AddTransport(new FakeTransportConfig(a));
                busA(bus);
            });
            m.AddBus("broker-b", bus =>
            {
                bus.AddTransport(new FakeTransportConfig(b));
                busB(bus);
            });
        });

        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    private static string Envelope(IHost host, Ping ping) =>
        host.Services.GetRequiredService<IMessageSerializer>().Serialize(ping, headers: null);

    [Fact]
    public async Task A_consumer_registered_on_both_buses_receives_its_topic_from_each()
    {
        var collector = new Collector();
        collector.Expect(2);
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        using var host = await StartHostAsync(
            collector, a, b,
            bus => bus.AddConsumer<PingConsumer>(),
            bus => bus.AddConsumer<PingConsumer>());

        await a.DeliverAsync("ping", Envelope(host, new Ping("from-a")));
        await b.DeliverAsync("ping", Envelope(host, new Ping("from-b")));

        await collector.Completed.WaitAsync(TimeSpan.FromSeconds(5));
        collector.Handled.ShouldBe(["from-a", "from-b"], ignoreOrder: true);

        await host.StopAsync();
    }

    [Fact]
    public async Task A_consumer_registered_on_one_bus_receives_only_that_bus_messages()
    {
        var collector = new Collector();
        collector.Expect(1);
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        using var host = await StartHostAsync(
            collector, a, b,
            bus => bus.AddConsumer<PingConsumer>(),
            _ => { });

        await a.DeliverAsync("ping", Envelope(host, new Ping("from-a")));
        await b.DeliverAsync("ping", Envelope(host, new Ping("from-b")));

        await collector.Completed.WaitAsync(TimeSpan.FromSeconds(5));
        // Give the (unsubscribed) second bus a chance to leak a delivery before asserting the negative.
        await Task.Delay(200);

        collector.Handled.ShouldBe(["from-a"]);

        await host.StopAsync();
    }

    [Fact]
    public async Task Resolving_the_unkeyed_bus_with_several_named_buses_and_no_default_is_an_error()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus("broker-a", bus => bus.AddTransport(new FakeTransportConfig(new RecordingTransport("broker-a"))));
            m.AddBus("broker-b", bus => bus.AddTransport(new FakeTransportConfig(new RecordingTransport("broker-b"))));
        });
        await using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(() => provider.GetRequiredService<IBus>());
        ex.Message.ShouldContain("broker-a");
        ex.Message.ShouldContain("broker-b");
    }

    [Fact]
    public async Task A_keyed_bus_publishes_to_its_own_transport_only()
    {
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus("broker-a", bus => bus.AddTransport(new FakeTransportConfig(a)));
            m.AddBus("broker-b", bus => bus.AddTransport(new FakeTransportConfig(b)));
        });
        await using var provider = services.BuildServiceProvider();

        await provider.GetRequiredKeyedService<IBus>("broker-b").PublishAsync("ping", new Ping("x"));

        b.Published.ShouldHaveSingleItem().Topic.ShouldBe("ping");
        a.Published.ShouldBeEmpty();
    }

    [Fact]
    public async Task Provider_GetBus_publishes_to_the_named_bus()
    {
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus("broker-a", bus => bus.AddTransport(new FakeTransportConfig(a)));
            m.AddBus("broker-b", bus => bus.AddTransport(new FakeTransportConfig(b)));
        });
        await using var provider = services.BuildServiceProvider();
        var buses = provider.GetRequiredService<IBusProvider>();

        await buses.GetBus("broker-a").PublishAsync("ping", new Ping("x"));

        a.Published.ShouldHaveSingleItem().Topic.ShouldBe("ping");
        b.Published.ShouldBeEmpty();
    }

    [Fact]
    public async Task Provider_lets_a_single_event_go_to_two_buses_masstransit_style()
    {
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus("broker-a", bus => bus.AddTransport(new FakeTransportConfig(a)));
            m.AddBus("broker-b", bus => bus.AddTransport(new FakeTransportConfig(b)));
        });
        await using var provider = services.BuildServiceProvider();
        var buses = provider.GetRequiredService<IBusProvider>();

        var evt = new Ping("both");
        await buses.GetBus("broker-a").PublishAsync("ping", evt);
        await buses.GetBus("broker-b").PublishAsync("ping", evt);

        a.Published.ShouldHaveSingleItem().Topic.ShouldBe("ping");
        b.Published.ShouldHaveSingleItem().Topic.ShouldBe("ping");
    }

    [Fact]
    public async Task Provider_returns_the_same_bus_instance_per_name()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
            m.AddBus("broker-a", bus => bus.AddTransport(new FakeTransportConfig(new RecordingTransport("broker-a")))));
        await using var provider = services.BuildServiceProvider();
        var buses = provider.GetRequiredService<IBusProvider>();

        buses.GetBus("broker-a").ShouldBeSameAs(buses.GetBus("broker-a"));
    }

    [Fact]
    public async Task Provider_GetBus_with_an_unknown_bus_name_throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
            m.AddBus("broker-a", bus => bus.AddTransport(new FakeTransportConfig(new RecordingTransport("broker-a")))));
        await using var provider = services.BuildServiceProvider();
        var buses = provider.GetRequiredService<IBusProvider>();

        var ex = Should.Throw<InvalidOperationException>(() => buses.GetBus("nope"));
        ex.Message.ShouldContain("nope");
        ex.Message.ShouldContain("broker-a");
    }

    [Fact]
    public async Task A_consumer_registered_in_a_bus_block_is_subscribed()
    {
        var collector = new Collector();
        collector.Expect(1);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(collector);
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<PingConsumer>();
        }));
        using var host = builder.Build();
        await host.StartAsync();

        await host.Services.GetRequiredService<IBus>().PublishAsync("ping", new Ping("scoped"));

        await collector.Completed.WaitAsync(TimeSpan.FromSeconds(5));
        collector.Handled.ShouldContain("scoped");

        await host.StopAsync();
    }

    [Fact]
    public async Task The_same_consumer_registered_on_two_buses_is_a_single_instance()
    {
        var collector = new Collector();
        collector.Expect(2);
        var a = new RecordingTransport("broker-a");
        var b = new RecordingTransport("broker-b");

        using var host = await StartHostAsync(
            collector, a, b,
            bus => bus.AddConsumer<PingConsumer>(),
            bus => bus.AddConsumer<PingConsumer>());

        // Registered against two buses, but a single instance handles both.
        host.Services.GetServices<PingConsumer>().Count().ShouldBe(1);

        await a.DeliverAsync("ping", Envelope(host, new Ping("from-a")));
        await b.DeliverAsync("ping", Envelope(host, new Ping("from-b")));

        await collector.Completed.WaitAsync(TimeSpan.FromSeconds(5));
        collector.Handled.ShouldBe(["from-a", "from-b"], ignoreOrder: true);

        await host.StopAsync();
    }
}
