using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runax.Messaging;
using Runax.Messaging.Abstractions;
using Runax.Messaging.AotSmoke;
using Runax.Messaging.Contracts.JsonSchema;
using Runax.Messaging.InMemory;
using Runax.Messaging.Outbox;
using Runax.Messaging.TestKit;
using Runax.Messaging.Transports.Aws.Sns;
using Runax.Messaging.Transports.Aws.Sqs;
using Runax.Messaging.Transports.Azure.EventHubs;
using Runax.Messaging.Transports.Azure.ServiceBus;
using Runax.Messaging.Transports.RabbitMq;
using Runax.Messaging.Transports.Redis;

var timeout = TimeSpan.FromSeconds(30);

try
{
    await RunHostAsync();
    await RunTestKitAsync();
    Console.WriteLine("AOT smoke test passed.");
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"AOT smoke test failed: {ex}");
    return 1;
}

// A generic host with a consuming bus (contract-versioned message, one retried delivery) and an
// outbox bus, both over the in-memory transport.
async Task RunHostAsync()
{
    var builder = Host.CreateApplicationBuilder(args);
    builder.Logging.SetMinimumLevel(LogLevel.Error);
    builder.Services.Configure<JsonSerializerOptions>(o => o.TypeInfoResolverChain.Insert(0, SmokeJsonContext.Default));
    builder.Services.AddSingleton<Probe>();

    builder.Services.AddRunaxMessaging(messaging =>
    {
        // Embedded contract schemas, validated on both sides (registry, evaluator, and formats under AOT).
        messaging.AddJsonSchemaContracts(c => c.AddEmbeddedSchemas(typeof(Probe).Assembly));

        messaging.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<OrderPlacedConsumer>();
            bus.WithRetry(o => o.InitialDelay = TimeSpan.FromMilliseconds(10));
            bus.ValidateContracts(v =>
            {
                v.OnPublish = ContractValidation.Enforce;
                v.OnConsume = ContractValidation.Enforce;
            });
        });

        messaging.AddBus("outbox", bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<InvoiceIssuedConsumer>();
            bus.AddOutbox(o => o.PollingInterval = TimeSpan.FromMilliseconds(50));
            bus.AddOutboxStore(new InMemoryOutboxStoreConfig());
        });

        // Each AOT-supported transport, so its code and its broker SDK go through the AOT compiler. The
        // buses are publish-only and never resolved, so nothing connects to a broker.
        AddUnusedBus(messaging, "rabbitmq", new RabbitMqConfig());
        AddUnusedBus(messaging, "sqs", new SqsConfig());
        AddUnusedBus(messaging, "sns", new SnsConfig());
        AddUnusedBus(messaging, "servicebus", new AzureServiceBusConfig { ConnectionString = "unused" });
        AddUnusedBus(messaging, "eventhubs", new AzureEventHubsConfig { ConnectionString = "unused" });
        AddUnusedBus(messaging, "redis", new RedisConfig { Configuration = "localhost:6379" });
    });

    using var host = builder.Build();
    await host.StartAsync();

    var probe = host.Services.GetRequiredService<Probe>();
    var buses = host.Services.GetRequiredService<IBusProvider>();

    await buses.GetBus(BusNames.Default).PublishAsync(
        "orders", new OrderPlaced(42, "widget"), new Dictionary<string, string> { ["source"] = "aot-smoke" });
    await buses.GetBus("outbox").PublishAsync("invoices", new InvoiceIssued(42, 9.99m));

    Expect(await probe.Orders.Task.WaitAsync(timeout), new OrderPlaced(42, "widget"));
    Expect(await probe.Invoices.Task.WaitAsync(timeout), new InvoiceIssued(42, 9.99m));

    // An invalid message never leaves the producer.
    try
    {
        await buses.GetBus(BusNames.Default).PublishAsync("orders", new OrderPlaced(0, "Widget"));
        throw new InvalidOperationException("Expected contract validation to reject the message.");
    }
    catch (ContractValidationException ex) when (ex.Errors.Count == 2)
    {
    }

    await host.StopAsync();
}

// The TestKit harness: recording transport, typed wait, and deserialization of what was recorded.
async Task RunTestKitAsync()
{
    await using var harness = await MessagingTestHarness.Create()
        .AddConsumer<InvoiceIssuedConsumer>()
        .AddService(new Probe())
        .ConfigureServices(s => s.Configure<JsonSerializerOptions>(
            o => o.TypeInfoResolverChain.Insert(0, SmokeJsonContext.Default)))
        .StartAsync();

    await harness.PublishAsync("invoices", new InvoiceIssued(7, 1.5m));
    Expect(await harness.WaitForConsumedAsync<InvoiceIssued>("invoices", timeout), new InvoiceIssued(7, 1.5m));
}

static void AddUnusedBus(MessagingConfigurator messaging, string name, TransportConfig config) =>
    messaging.AddBus(name, bus =>
    {
        bus.Mode = BusMode.PublishOnly;
        bus.AddTransport(config);
    });

static void Expect<T>(T actual, T expected)
{
    if (!EqualityComparer<T>.Default.Equals(actual, expected))
        throw new InvalidOperationException($"Expected {expected}, got {actual}.");
}
