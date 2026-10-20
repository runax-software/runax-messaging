using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.InMemory;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class NativeHeadersTests
{
    [MessageContract(2, Name = "orders.placed")]
    public sealed record OrderPlaced(int Id);

    private static readonly Dictionary<string, string> TenantHeader = new() { ["tenant"] = "acme" };

    // --- Publishing ---------------------------------------------------------------------------

    [Fact]
    public async Task Publish_writes_headers_and_contract_metadata_as_native_headers()
    {
        var transport = new CapturingTransport();
        await using var provider = BuildProvider(transport);

        await provider.GetRequiredService<IBus>().PublishAsync("orders", new OrderPlaced(1), TenantHeader);

        var message = transport.Published.ShouldHaveSingleItem();
        message.Topic.ShouldBe("orders");
        message.Headers.ShouldContainKeyAndValue("tenant", "acme");
        message.Headers.ShouldContainKeyAndValue(MessageHeaders.ContractName, "orders.placed");
        message.Headers.ShouldContainKeyAndValue(MessageHeaders.ContractVersion, "2");

        // The envelope keeps its own copy.
        message.Body.ShouldContain("\"__runax\"");
        message.Body.ShouldContain("\"tenant\":\"acme\"");
    }

    [Fact]
    public async Task A_bus_that_turns_native_headers_off_keeps_them_in_the_envelope_only()
    {
        var transport = new CapturingTransport();
        await using var provider = BuildProvider(transport, bus => bus.WriteNativeHeaders = false);

        await provider.GetRequiredService<IBus>().PublishAsync("orders", new OrderPlaced(1), TenantHeader);

        var message = transport.Published.ShouldHaveSingleItem();
        message.Headers.ShouldBeEmpty();
        message.Body.ShouldContain("\"tenant\":\"acme\"");
        message.Body.ShouldContain("\"contract_version\":2");
    }

    [Fact]
    public async Task Batch_publish_writes_native_headers_on_every_message()
    {
        var transport = new CapturingTransport();
        await using var provider = BuildProvider(transport);

        await provider.GetRequiredService<IBus>().PublishBatchAsync("orders", [new OrderPlaced(1), new OrderPlaced(2)]);

        transport.Published.Count.ShouldBe(2);
        transport.Published.ShouldAllBe(m => m.Headers[MessageHeaders.ContractVersion] == "2");
    }

    // --- Merging on receive ---------------------------------------------------------------------

    [Fact]
    public void By_default_the_broker_header_wins_and_headers_from_both_sides_are_kept()
    {
        var context = Context(new Dictionary<string, string> { ["shared"] = "envelope", ["envelope-only"] = "e" });

        var merged = NativeHeaders.Merge(
            context, new Dictionary<string, string> { ["shared"] = "broker", ["broker-only"] = "b" }, HeaderSource.Broker);

        merged.Headers["shared"].ShouldBe("broker");
        merged.Headers["envelope-only"].ShouldBe("e");
        merged.Headers["broker-only"].ShouldBe("b");
    }

    [Fact]
    public void The_envelope_copy_wins_when_the_bus_prefers_it()
    {
        var context = Context(new Dictionary<string, string> { ["shared"] = "envelope" }, contractVersion: 1);

        var merged = NativeHeaders.Merge(
            context,
            new Dictionary<string, string> { ["shared"] = "broker", [MessageHeaders.ContractVersion] = "2" },
            HeaderSource.Envelope);

        merged.Headers["shared"].ShouldBe("envelope");
        merged.ContractVersion.ShouldBe(1);
    }

    [Fact]
    public void Contract_metadata_is_read_from_native_headers_and_not_repeated_in_the_headers()
    {
        var merged = NativeHeaders.Merge(
            Context([]),
            new Dictionary<string, string>
            {
                [MessageHeaders.ContractName] = "orders.placed",
                [MessageHeaders.ContractVersion] = "3",
            },
            HeaderSource.Broker);

        merged.ContractName.ShouldBe("orders.placed");
        merged.ContractVersion.ShouldBe(3);
        merged.Headers.ShouldBeEmpty();
    }

    [Fact]
    public void A_broker_contract_version_overrides_the_envelope_one_by_default()
    {
        var merged = NativeHeaders.Merge(
            Context([], contractVersion: 1),
            new Dictionary<string, string> { [MessageHeaders.ContractVersion] = "2" },
            HeaderSource.Broker);

        merged.ContractVersion.ShouldBe(2);
    }

    [Fact]
    public void A_malformed_native_contract_version_is_ignored()
    {
        var merged = NativeHeaders.Merge(
            Context([], contractVersion: 1),
            new Dictionary<string, string> { [MessageHeaders.ContractVersion] = "v2" },
            HeaderSource.Broker);

        merged.ContractVersion.ShouldBe(1);
    }

    [Fact]
    public void Native_headers_are_rebuilt_from_a_serialized_envelope()
    {
        var envelope = new EnvelopeSerializer(new SystemTextJsonSerializer()).Serialize(new OrderPlaced(1), TenantHeader);

        var headers = NativeHeaders.FromEnvelope(envelope);

        headers.ShouldContainKeyAndValue("tenant", "acme");
        headers.ShouldContainKeyAndValue(MessageHeaders.ContractName, "orders.placed");
        headers.ShouldContainKeyAndValue(MessageHeaders.ContractVersion, "2");
    }

    // --- End to end over the in-memory transport --------------------------------------------------

    [Fact]
    public async Task A_foreign_message_is_routed_by_its_native_contract_version_and_sees_its_native_headers()
    {
        var captured = new TaskCompletionSource<MessageContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(captured);
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<V1Consumer>();
            bus.AddConsumer<V2Consumer>();
        }));
        using var host = builder.Build();
        await host.StartAsync();

        // A producer outside this library: plain JSON body, metadata only as native headers.
        var transport = host.Services.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);
        await transport.PublishAsync(new TransportMessage(
            "orders",
            """{"Id":7}""",
            new Dictionary<string, string> { [MessageHeaders.ContractVersion] = "2", ["tenant"] = "acme" }));

        var context = await captured.Task.WaitAsync(TimeSpan.FromSeconds(5));
        context.ContractVersion.ShouldBe(2);
        context.Headers["tenant"].ShouldBe("acme");
        context.Deserialize<OrderPlaced>()!.Id.ShouldBe(7);

        await host.StopAsync();
    }

    [Fact]
    public async Task A_dead_lettered_copy_keeps_the_native_headers_and_adds_the_dead_letter_reason()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<PoisonConsumer>();
        }));
        using var host = builder.Build();
        await host.StartAsync();

        var transport = host.Services.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);
        var deadLettered = new TaskCompletionSource<TransportMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = new CancellationTokenSource();
        var listener = transport.SubscribeAsync(["orders.dead-letter"], message =>
        {
            deadLettered.TrySetResult(message);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, stop.Token);

        await transport.PublishAsync(new TransportMessage("orders", """{"Id":7}""", TenantHeader));

        var message = await deadLettered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        message.Headers.ShouldContainKeyAndValue("tenant", "acme");
        message.Headers.ShouldContainKeyAndValue("x-runax-dlq-original-topic", "orders");
        message.Headers.ShouldContainKey("x-runax-dlq-reason");

        await stop.CancelAsync();
        await listener;
        await host.StopAsync();
    }

    // --- Helpers ----------------------------------------------------------------------------------

    private static ServiceProvider BuildProvider(IMessagingTransport transport, Action<BusBuilder>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new FakeTransportConfig(transport));
            configure?.Invoke(bus);
        }));
        return services.BuildServiceProvider();
    }

    private static MessageContext Context(Dictionary<string, string> headers, int? contractVersion = null) => new()
    {
        Topic = "orders",
        Body = "{}",
        Headers = headers,
        ContractVersion = contractVersion,
    };

    private sealed class CapturingTransport : IMessagingTransport
    {
        private readonly ConcurrentQueue<TransportMessage> _published = new();

        public IReadOnlyList<TransportMessage> Published => [.. _published];

        public string SystemName => "capturing";

        public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default) =>
            PublishAsync(new TransportMessage(topic, envelopeJson), cancellationToken);

        public ValueTask PublishAsync(TransportMessage message, CancellationToken cancellationToken = default)
        {
            _published.Enqueue(message);
            return ValueTask.CompletedTask;
        }

        public ValueTask PublishBatchAsync(
            IReadOnlyList<TransportMessage> messages,
            CancellationToken cancellationToken = default)
        {
            foreach (var message in messages)
                _published.Enqueue(message);
            return ValueTask.CompletedTask;
        }

        public Task SubscribeAsync(
            string[] topics,
            Func<string, string, ValueTask<MessageDisposition>> onMessage,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
    }

    private sealed class V1Consumer : IMessageConsumer
    {
        public string Topic => "orders";

        public int? ContractVersion => 1;

        public ValueTask HandleAsync(MessageContext context, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("The v1 consumer must not receive a v2 message.");
    }

    private sealed class V2Consumer(TaskCompletionSource<MessageContext> captured) : IMessageConsumer
    {
        public string Topic => "orders";

        public int? ContractVersion => 2;

        public ValueTask HandleAsync(MessageContext context, CancellationToken cancellationToken = default)
        {
            captured.TrySetResult(context);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PoisonConsumer : IMessageConsumer
    {
        public string Topic => "orders";

        public int? ContractVersion => null;

        public ValueTask HandleAsync(MessageContext context, CancellationToken cancellationToken = default) =>
            throw new PoisonMessageException("Rejected by the test.");
    }
}
