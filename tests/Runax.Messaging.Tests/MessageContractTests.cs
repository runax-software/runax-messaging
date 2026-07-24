using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.InMemory;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class MessageContractTests
{
    private const string Topic = "orders.placed";

    [MessageContract(1)]
    private sealed record OrderV1(int Id);

    [MessageContract(2)]
    private sealed record OrderV2(int Id, string Currency);

    private sealed record OrderPlain(int Id);   // no contract → unversioned

    private sealed class Recorder
    {
        private readonly ConcurrentDictionary<string, TaskCompletionSource> _waits = new();

        public ConcurrentBag<(string Label, string Payload)> Received { get; } = [];

        public void Record(string label, string payload)
        {
            Received.Add((label, payload));
            Signal(label).TrySetResult();
        }

        public Task Wait(string label) => Signal(label).Task;

        private TaskCompletionSource Signal(string label) =>
            _waits.GetOrAdd(label, static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
    }

    private sealed class OrderV1Consumer(Recorder recorder) : MessageConsumer<OrderV1>
    {
        public override string Topic => MessageContractTests.Topic;
        protected override ValueTask HandleAsync(OrderV1 message, CancellationToken ct)
        {
            recorder.Record("v1", message.Id.ToString());
            return ValueTask.CompletedTask;
        }
    }

    private sealed class OrderV2Consumer(Recorder recorder) : MessageConsumer<OrderV2>
    {
        public override string Topic => MessageContractTests.Topic;
        protected override ValueTask HandleAsync(OrderV2 message, CancellationToken ct)
        {
            recorder.Record("v2", message.Id.ToString());
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PlainConsumer(Recorder recorder) : MessageConsumer<OrderPlain>
    {
        public override string Topic => MessageContractTests.Topic;
        protected override ValueTask HandleAsync(OrderPlain message, CancellationToken ct)
        {
            recorder.Record("plain", message.Id.ToString());
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DlqConsumer(Recorder recorder) : MessageConsumer<OrderPlain>
    {
        public override string Topic => "orders.placed.dead-letter";
        protected override ValueTask HandleAsync(OrderPlain message, CancellationToken ct)
        {
            recorder.Record("dlq", message.Id.ToString());
            return ValueTask.CompletedTask;
        }
    }

    private sealed record S3Event(string Bucket);   // a foreign shape, no [MessageContract]

    private sealed class S3EventConsumer(Recorder recorder) : MessageConsumer<S3Event>
    {
        public override string Topic => "s3-events";
        protected override ValueTask HandleAsync(S3Event message, CancellationToken ct)
        {
            recorder.Record("s3", message.Bucket);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingUnroutableHandler(Recorder recorder) : IUnroutableMessageHandler
    {
        public ValueTask<MessageDisposition> HandleAsync(UnroutableMessage message, CancellationToken ct)
        {
            recorder.Record("unroutable", $"{message.Topic}:{message.ContractVersion}");
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }
    }

    private static async Task<IHost> StartAsync(Recorder recorder, Action<MessagingConfigurator> configure)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(recorder);
        builder.Services.AddRunaxMessaging(configure);
        var host = builder.Build();
        await host.StartAsync();
        return host;
    }

    [Fact]
    public void Serializer_stamps_contract_from_the_attribute_and_leaves_unversioned_types_null()
    {
        var serializer = new EnvelopeSerializer(new SystemTextJsonSerializer());

        serializer.Deserialize(serializer.Serialize(new OrderV1(1), null), Topic).ContractVersion.ShouldBe(1);
        serializer.Deserialize(serializer.Serialize(new OrderPlain(1), null), Topic).ContractVersion.ShouldBeNull();

        // The version rides under the reserved metadata key, not at the top level.
        using var doc = JsonDocument.Parse(serializer.Serialize(new OrderV1(1), null));
        doc.RootElement.GetProperty(EnvelopeSerializer.MetadataKey).GetProperty("contract_version").GetInt32().ShouldBe(1);
    }

    [Fact]
    public async Task Each_version_is_delivered_only_to_the_consumer_that_declares_it()
    {
        var recorder = new Recorder();
        using var host = await StartAsync(recorder, m => m
            .AddInMemory()
            .AddConsumer<OrderV1Consumer>()
            .AddConsumer<OrderV2Consumer>());
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        await publisher.PublishAsync(Topic, new OrderV1(1));
        await recorder.Wait("v1").WaitAsync(TimeSpan.FromSeconds(5));

        await publisher.PublishAsync(Topic, new OrderV2(2, "USD"));
        await recorder.Wait("v2").WaitAsync(TimeSpan.FromSeconds(5));

        recorder.Received.ShouldContain(("v1", "1"));
        recorder.Received.ShouldContain(("v2", "2"));
        recorder.Received.ShouldNotContain(("v1", "2"));  // v1 consumer never saw the v2 message
        recorder.Received.ShouldNotContain(("v2", "1"));

        await host.StopAsync();
    }

    [Fact]
    public async Task An_unversioned_consumer_still_receives_a_versioned_message()
    {
        var recorder = new Recorder();
        using var host = await StartAsync(recorder, m => m.AddInMemory().AddConsumer<PlainConsumer>());
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        await publisher.PublishAsync(Topic, new OrderV1(5));
        await recorder.Wait("plain").WaitAsync(TimeSpan.FromSeconds(5));

        recorder.Received.ShouldContain(("plain", "5"));

        await host.StopAsync();
    }

    [Fact]
    public async Task An_unroutable_version_is_dead_lettered_by_default()
    {
        var recorder = new Recorder();
        using var host = await StartAsync(recorder, m => m
            .AddInMemory()
            .AddConsumer<OrderV2Consumer>()   // only v2 is handled
            .AddConsumer<DlqConsumer>());     // capture the dead-letter topic
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        await publisher.PublishAsync(Topic, new OrderV1(7));   // v1 has no consumer
        await recorder.Wait("dlq").WaitAsync(TimeSpan.FromSeconds(5));

        recorder.Received.ShouldContain(("dlq", "7"));
        recorder.Received.ShouldNotContain(("v2", "7"));

        await host.StopAsync();
    }

    [Fact]
    public async Task A_custom_unroutable_handler_receives_the_message()
    {
        var recorder = new Recorder();
        using var host = await StartAsync(recorder, m => m
            .AddInMemory()
            .AddConsumer<OrderV2Consumer>()
            .OnUnroutableMessage<RecordingUnroutableHandler>());
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();

        await publisher.PublishAsync(Topic, new OrderV1(9));
        await recorder.Wait("unroutable").WaitAsync(TimeSpan.FromSeconds(5));

        recorder.Received.ShouldContain(("unroutable", $"{Topic}:1"));

        await host.StopAsync();
    }

    [Fact]
    public async Task A_foreign_message_with_no_envelope_is_consumed_as_a_raw_body()
    {
        var recorder = new Recorder();
        using var host = await StartAsync(recorder, m => m.AddInMemory().AddConsumer<S3EventConsumer>());

        // Deliver a payload straight to the transport as an external producer would — no __runax key.
        var transport = host.Services.GetRequiredService<IMessagingTransport>();
        await transport.PublishAsync("s3-events", """{"Bucket":"my-bucket"}""");

        await recorder.Wait("s3").WaitAsync(TimeSpan.FromSeconds(5));
        recorder.Received.ShouldContain(("s3", "my-bucket"));

        await host.StopAsync();
    }

    [Fact]
    public async Task The_catalog_reports_handled_topics_and_versions()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(new Recorder());
        services.AddRunaxMessaging(m => m
            .AddInMemory()
            .AddConsumer<OrderV1Consumer>()
            .AddConsumer<OrderV2Consumer>());
        await using var provider = services.BuildServiceProvider();

        var catalog = provider.GetRequiredService<IMessageContractCatalog>();

        catalog.Handled.Select(h => h.Version).OrderBy(v => v).ShouldBe([1, 2]);
        catalog.Accepts(Topic, 1).ShouldBeTrue();
        catalog.Accepts(Topic, 3).ShouldBeFalse();
    }

    [Fact]
    public async Task Built_in_unroutable_handlers_map_to_their_dispositions()
    {
        var message = new UnroutableMessage
        {
            Topic = Topic,
            ContractVersion = 1,
            Body = "{}",
            Headers = new Dictionary<string, string>(),
            TransportSystemName = "in-memory",
        };

        (await new DeadLetterUnroutableHandler().HandleAsync(message)).ShouldBe(MessageDisposition.DeadLetter);
        (await new RequeueUnroutableHandler().HandleAsync(message)).ShouldBe(MessageDisposition.Requeue);
        (await new DiscardUnroutableHandler().HandleAsync(message)).ShouldBe(MessageDisposition.Acknowledge);
    }
}
