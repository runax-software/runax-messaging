using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.InMemory;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class ContractSchemaTests
{
    private const string V2 = "https://contracts.test/orders/order-placed/v2";
    private const string V3 = "https://contracts.test/orders/order-placed/v3";

    [MessageContract(2, Name = "orders.placed", Schema = V2)]
    public sealed record OrderPlacedV2(int Id);

    [MessageContract(3, Schema = V3 + "#", Reads = [V2])]
    public sealed record OrderPlacedV3(int Id);

    [MessageContract(1, Reads = [V2])]
    public sealed record ReadsWithoutSchema(int Id);

    private readonly EnvelopeSerializer _serializer = new(new SystemTextJsonSerializer());

    // --- Wire ----------------------------------------------------------------------------------

    [Fact]
    public void The_schema_id_is_written_to_the_envelope_and_read_back()
    {
        var envelope = _serializer.Serialize(new OrderPlacedV2(1), headers: null);

        envelope.ShouldContain($"\"contract_schema\":\"{V2}\"");
        _serializer.Deserialize(envelope, "orders").ContractSchema.ShouldBe(V2);
    }

    [Fact]
    public void An_empty_trailing_fragment_is_dropped()
    {
        _serializer.Serialize(new OrderPlacedV3(1), headers: null).ShouldContain($"\"contract_schema\":\"{V3}\"");
        _serializer.Deserialize("{\"Id\":1,\"__runax\":{\"contract_schema\":\"" + V2 + "#\"}}", "orders")
            .ContractSchema.ShouldBe(V2);
    }

    [Fact]
    public void A_non_string_schema_id_is_ignored_rather_than_failing_the_envelope()
    {
        var context = _serializer.Deserialize("""{"Id":1,"__runax":{"contract_schema":42,"contract_version":2}}""", "orders");

        context.ContractSchema.ShouldBeNull();
        context.ContractVersion.ShouldBe(2);
    }

    [Fact]
    public void The_schema_id_is_a_native_header_and_merges_like_the_other_contract_headers()
    {
        NativeHeaders.Build(MessageContractCache.For(typeof(OrderPlacedV2)), headers: null)
            .ShouldContainKeyAndValue(MessageHeaders.ContractSchema, V2);

        var context = new MessageContext
        {
            Topic = "orders",
            Body = "{}",
            Headers = new Dictionary<string, string>(),
            ContractSchema = V2,
        };
        var native = new Dictionary<string, string> { [MessageHeaders.ContractSchema] = V3 };

        NativeHeaders.Merge(context, native, HeaderSource.Broker).ContractSchema.ShouldBe(V3);
        NativeHeaders.Merge(context, native, HeaderSource.Envelope).ContractSchema.ShouldBe(V2);
        NativeHeaders.Merge(context, native, HeaderSource.Broker).Headers.ShouldBeEmpty();
    }

    [Fact]
    public void A_type_that_reads_lines_without_its_own_schema_is_rejected()
    {
        var ex = Should.Throw<InvalidOperationException>(() => MessageContractCache.SchemasFor(typeof(ReadsWithoutSchema)));

        ex.Message.ShouldContain(nameof(ReadsWithoutSchema));
    }

    [Fact]
    public void A_consumer_type_accepts_its_schema_and_the_lines_it_reads()
    {
        MessageContractCache.SchemasFor(typeof(OrderPlacedV3)).ShouldBe([V3, V2], ignoreOrder: true);
        MessageContractCache.SchemasFor(typeof(MessageContextTests.Order)).ShouldBeEmpty();
    }

    // --- Catalog -------------------------------------------------------------------------------

    [Fact]
    public void The_catalog_lists_schema_lines_and_answers_by_the_dispatch_rule()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<V3Consumer>();
        }));
        using var provider = services.BuildServiceProvider();
        var catalog = provider.GetRequiredService<IMessageContractCatalog>();

        catalog.Handled.ShouldContain(new HandledContract("orders", 3) { Schema = V3 });
        catalog.Handled.ShouldContain(h => h.Topic == "orders" && h.Schema == V2);
        catalog.Accepts("orders", V3).ShouldBeTrue();
        catalog.Accepts("orders", V2).ShouldBeTrue();
        catalog.Accepts("orders", "https://contracts.test/other/v1").ShouldBeFalse();
        catalog.Accepts("orders", 3).ShouldBeTrue();
        catalog.Accepts("orders", "https://contracts.test/other/v1", 3).ShouldBeFalse();
    }

    // --- Unroutable details --------------------------------------------------------------------

    [Fact]
    public void The_dead_letter_reason_names_the_schema_and_keeps_long_details_within_512_bytes()
    {
        new UnroutableMessageException("orders", 2, V2).Message.ShouldContain($"contract schema '{V2}'");

        var detail = string.Join("; ", Enumerable.Range(0, 50).Select(i => $"{V2} at /field{i}: required"));
        var message = new UnroutableMessageException("orders", null, null, UnroutableReason.NoMatchingSchema, detail).Message;

        message.ShouldEndWith("…");
        Encoding.UTF8.GetByteCount(message).ShouldBeLessThan(700);
    }

    // --- End to end ----------------------------------------------------------------------------

    [Fact]
    public async Task An_unidentified_message_reaches_the_consumer_its_body_validates_for()
    {
        var received = new TaskCompletionSource<MessageContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddSingleton<IContractSchemaMatcher>(new AcceptingMatcher(V2));
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<CapturingV2Consumer>();
        }));
        using var host = builder.Build();
        await host.StartAsync();

        var transport = host.Services.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);
        await transport.PublishAsync("orders", """{"Id":7}""");

        var context = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        context.ContractSchema.ShouldBeNull();
        context.InferredContractSchemas.ShouldBe([V2]);

        await host.StopAsync();
    }

    [Fact]
    public async Task The_unroutable_handler_sees_the_schema_and_the_reason()
    {
        var received = new TaskCompletionSource<UnroutableMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<V3Consumer>();
            bus.OnUnroutableMessage<CapturingUnroutableHandler>();
        }));
        using var host = builder.Build();
        await host.StartAsync();

        var transport = host.Services.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);
        await transport.PublishAsync(new TransportMessage(
            "orders",
            """{"Id":1}""",
            new Dictionary<string, string> { [MessageHeaders.ContractSchema] = "https://contracts.test/other/v1" }));

        var unroutable = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        unroutable.ContractSchema.ShouldBe("https://contracts.test/other/v1");
        unroutable.Reason.ShouldBe(UnroutableReason.NoMatchingContract);

        await host.StopAsync();
    }

    private sealed class V3Consumer : MessageConsumer<OrderPlacedV3>
    {
        public override string Topic => "orders";

        protected override ValueTask HandleAsync(OrderPlacedV3 message, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
    }

    private sealed class CapturingV2Consumer(TaskCompletionSource<MessageContext> received) : IMessageConsumer
    {
        public string Topic => "orders";

        public int? ContractVersion => 2;

        public string? ContractSchema => V2;

        public System.Collections.Frozen.FrozenSet<string> ContractSchemas { get; } =
            System.Collections.Frozen.FrozenSet.ToFrozenSet([V2], StringComparer.Ordinal);

        public ValueTask HandleAsync(MessageContext context, CancellationToken cancellationToken = default)
        {
            received.TrySetResult(context);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class AcceptingMatcher(string line) : IContractSchemaMatcher
    {
        public int? LegacyVersionOf(string contractSchema) => null;

        public ContractMatch Match(string body, IReadOnlyCollection<string> candidates) =>
            new(candidates.Contains(line) ? [line] : [], []);
    }

    private sealed class CapturingUnroutableHandler(TaskCompletionSource<UnroutableMessage> received)
        : IUnroutableMessageHandler
    {
        public ValueTask<MessageDisposition> HandleAsync(UnroutableMessage message, CancellationToken cancellationToken = default)
        {
            received.TrySetResult(message);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }
    }
}
