using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Contracts.JsonSchema;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Contracts.JsonSchema.Tests;

public class JsonSchemaContractsTests
{
    [MessageContract(2, Schema = Ids.PlacedV2)]
    public sealed record OrderPlacedV2(string Kind, int Id, string Currency, string LegacyCode);

    [MessageContract(3, Schema = Ids.PlacedV3, Reads = [Ids.PlacedV2])]
    public sealed record OrderPlacedV3(string Kind, int Id, string Currency);

    [MessageContract(1, Schema = Ids.CancelledV1)]
    public sealed record OrderCancelledV1(string Kind, int Id, string? Reason);

    [MessageContract(1, Schema = Ids.AnythingV1)]
    public sealed record Anything(string? Note);

    private sealed class Inbox
    {
        public TaskCompletionSource<object> Placed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<object> Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class PlacedConsumer(Inbox inbox) : MessageConsumer<OrderPlacedV3>
    {
        public override string Topic => "orders";

        protected override ValueTask HandleAsync(OrderPlacedV3 message, CancellationToken cancellationToken)
        {
            inbox.Placed.TrySetResult(message);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CancelledConsumer(Inbox inbox) : MessageConsumer<OrderCancelledV1>
    {
        public override string Topic => "orders";

        protected override ValueTask HandleAsync(OrderCancelledV1 message, CancellationToken cancellationToken)
        {
            inbox.Cancelled.TrySetResult(message);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class AnythingConsumer : MessageConsumer<Anything>
    {
        public override string Topic => "misc";

        protected override ValueTask HandleAsync(Anything message, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
    }

    // --- Routing --------------------------------------------------------------------------------

    [Fact]
    public async Task Unidentified_messages_are_routed_by_the_schema_their_body_matches()
    {
        await using var app = await App.StartAsync(bus =>
        {
            bus.AddConsumer<PlacedConsumer>();
            bus.AddConsumer<CancelledConsumer>();
        });

        // A producer outside this library: plain JSON, no contract headers.
        await app.Transport.PublishAsync("orders", """{"kind":"order.cancelled","id":7,"reason":null}""");
        await app.Transport.PublishAsync("orders", """{"kind":"order.placed","id":8,"currency":"EUR"}""");

        (await app.Inbox.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5))).ShouldBe(new OrderCancelledV1("order.cancelled", 7, null));
        (await app.Inbox.Placed.Task.WaitAsync(TimeSpan.FromSeconds(5))).ShouldBe(new OrderPlacedV3("order.placed", 8, "EUR"));
    }

    [Fact]
    public async Task A_consumer_accepts_the_legacy_version_recorded_by_a_line_it_reads()
    {
        await using var app = await App.StartAsync(bus => bus.AddConsumer<PlacedConsumer>());

        // A 2.x producer: integer only, v2 shape.
        await app.Transport.PublishAsync(new TransportMessage(
            "orders",
            """{"kind":"order.placed","id":9,"currency":"USD","legacyCode":"L"}""",
            new Dictionary<string, string> { [MessageHeaders.ContractVersion] = "2" }));

        (await app.Inbox.Placed.Task.WaitAsync(TimeSpan.FromSeconds(5))).ShouldBe(new OrderPlacedV3("order.placed", 9, "USD"));
    }

    [Fact]
    public async Task A_body_no_schema_accepts_is_unroutable_with_the_failure_in_the_dead_letter_reason()
    {
        await using var app = await App.StartAsync(bus => bus.AddConsumer<PlacedConsumer>());
        var deadLetter = app.ListenTo("orders.dead-letter");

        await app.Transport.PublishAsync("orders", """{"kind":"order.placed","id":0,"currency":"EUR"}""");

        var message = await deadLetter.WaitAsync(TimeSpan.FromSeconds(5));
        message.Headers["x-runax-dlq-reason"].ShouldContain($"{Ids.PlacedV3} at /id: must be at least 1");
    }

    // --- Validation -----------------------------------------------------------------------------

    [Fact]
    public async Task Enforced_publish_validation_rejects_an_invalid_message_before_it_is_sent()
    {
        await using var app = await App.StartAsync(bus => bus.ValidateContracts(v => v.OnPublish = ContractValidation.Enforce));

        var ex = await Should.ThrowAsync<ContractValidationException>(
            app.Bus.PublishAsync("orders", new OrderPlacedV3("order.placed", 1, "EURO")).AsTask());

        ex.ContractSchema.ShouldBe(Ids.PlacedV3);
        ex.Errors.ShouldBe(["/currency: must be at most 3 characters"]);
    }

    [Fact]
    public async Task Enforced_consume_validation_dead_letters_an_invalid_body_as_poison()
    {
        await using var app = await App.StartAsync(bus =>
        {
            bus.AddConsumer<PlacedConsumer>();
            bus.ValidateContracts(v => v.OnConsume = ContractValidation.Enforce);
        });
        var deadLetter = app.ListenTo("orders.dead-letter");

        await app.Transport.PublishAsync(new TransportMessage(
            "orders",
            """{"kind":"order.placed","id":5,"currency":"EURO"}""",
            new Dictionary<string, string> { [MessageHeaders.ContractSchema] = Ids.PlacedV3 }));

        var message = await deadLetter.WaitAsync(TimeSpan.FromSeconds(5));
        message.Headers["x-runax-dlq-exception"].ShouldBe(typeof(ContractValidationException).FullName);
        message.Headers["x-runax-dlq-attempts"].ShouldBe("1");
    }

    [Fact]
    public async Task Reported_validation_logs_and_still_delivers()
    {
        await using var app = await App.StartAsync(bus =>
        {
            bus.AddConsumer<PlacedConsumer>();
            bus.ValidateContracts(v => v.OnConsume = ContractValidation.Report);
        });

        await app.Transport.PublishAsync(new TransportMessage(
            "orders",
            """{"kind":"order.placed","id":5,"currency":"EURO"}""",
            new Dictionary<string, string> { [MessageHeaders.ContractSchema] = Ids.PlacedV3 }));

        await app.Inbox.Placed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        app.Logs.GetSnapshot().ShouldContain(r =>
            r.Level == LogLevel.Warning && r.Message.Contains("failed validation against contract schema"));
    }

    [Fact]
    public async Task Consume_validation_accepts_a_body_valid_under_a_read_line()
    {
        await using var app = await App.StartAsync(bus =>
        {
            bus.AddConsumer<PlacedConsumer>();
            bus.ValidateContracts(v => v.OnConsume = ContractValidation.Enforce);
        });

        await app.Transport.PublishAsync(new TransportMessage(
            "orders",
            """{"kind":"order.placed","id":5,"currency":"EUR","legacyCode":"L"}""",
            new Dictionary<string, string> { [MessageHeaders.ContractSchema] = Ids.PlacedV2 }));

        (await app.Inbox.Placed.Task.WaitAsync(TimeSpan.FromSeconds(5))).ShouldBe(new OrderPlacedV3("order.placed", 5, "EUR"));
    }

    [Fact]
    public void Validating_without_loaded_schemas_fails_with_a_clear_error()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.ValidateContracts(v => v.OnPublish = ContractValidation.Enforce);
        }));
        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(() =>
            provider.GetRequiredService<IBus>().PublishAsync("orders", new OrderPlacedV3("order.placed", 1, "EUR")).AsTask().GetAwaiter().GetResult());
        ex.Message.ShouldContain("AddJsonSchemaContracts");
    }

    // --- Startup warnings -----------------------------------------------------------------------

    [Fact]
    public async Task Startup_warns_about_non_selective_lines()
    {
        await using var app = await App.StartAsync(bus => bus.AddConsumer<AnythingConsumer>());

        app.Logs.GetSnapshot().ShouldContain(r => r.Message.Contains($"'{Ids.AnythingV1}'") && r.Message.Contains("accepts any JSON object"));
    }

    [Fact]
    public async Task Startup_does_not_warn_about_lines_a_discriminator_tells_apart()
    {
        await using var app = await App.StartAsync(bus =>
        {
            bus.AddConsumer<PlacedConsumer>();
            bus.AddConsumer<CancelledConsumer>();
        });

        app.Logs.GetSnapshot().ShouldNotContain(r => r.Message.Contains("could both accept the same message"));
    }

    // --- Host -----------------------------------------------------------------------------------

    private sealed class App : IAsyncDisposable
    {
        private readonly IHost _host;
        private readonly CancellationTokenSource _stop = new();
        private readonly List<Task> _listeners = [];

        private App(IHost host)
        {
            _host = host;
            Inbox = host.Services.GetRequiredService<Inbox>();
            Transport = host.Services.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);
            Bus = host.Services.GetRequiredService<IBus>();
            Logs = host.Services.GetFakeLogCollector();
        }

        public Inbox Inbox { get; }

        public IMessagingTransport Transport { get; }

        public IBus Bus { get; }

        public FakeLogCollector Logs { get; }

        public static async Task<App> StartAsync(Action<BusBuilder> configure)
        {
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders().AddFakeLogging();
            builder.Services.AddSingleton<Inbox>();
            builder.Services.AddRunaxMessaging(messaging =>
            {
                messaging.AddJsonSchemaContracts(c => c.AddEmbeddedSchemas(typeof(Ids).Assembly));
                messaging.AddBus(bus =>
                {
                    bus.AddTransport(new InMemoryConfig());
                    bus.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
                    configure(bus);
                });
            });

            var host = builder.Build();
            await host.StartAsync();
            return new App(host);
        }

        public Task<TransportMessage> ListenTo(string topic)
        {
            var received = new TaskCompletionSource<TransportMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            _listeners.Add(Transport.SubscribeAsync([topic], message =>
            {
                received.TrySetResult(message);
                return ValueTask.FromResult(MessageDisposition.Acknowledge);
            }, _stop.Token));
            return received.Task;
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            await Task.WhenAll(_listeners);
            await _host.StopAsync();
            _host.Dispose();
            _stop.Dispose();
        }
    }
}
