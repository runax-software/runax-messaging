# Migrating from 1.x to 2.0

2.0 restructures configuration around **buses**: a bus is a named, self-contained messaging
context wrapping **exactly one transport**, plus the consumers, serialization, retry policy, and
(optionally) outbox around it. Talking to several brokers means registering several buses.

**The wire format is unchanged.** The `__runax` envelope, dead-letter headers, and retry
semantics are identical, so 1.x and 2.0 services interoperate on the same broker — you can
migrate services one at a time.

## The shape of the change

```csharp
// 1.x
services.AddRunaxMessaging(m =>
{
    m.AddRabbitMq(rabbit =>
    {
        rabbit.Configure(o => o.HostName = "broker");
        rabbit.AddConsumer<OrderConsumer>();
    });
    m.AddKafka(config.GetSection("Kafka"));
    m.PublishTo("rabbitmq");
    m.WithRetry(o => o.MaxAttempts = 5);
});

// 2.0 — one bus per transport
services.AddRunaxMessaging(m =>
{
    m.AddBus(bus =>                       // default bus ≙ the old PublishTo target
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "broker" });
        bus.AddConsumer<OrderConsumer>();
        bus.WithRetry(o => o.MaxAttempts = 5);
    });
    m.AddBus("kafka", bus =>
    {
        bus.AddTransport<KafkaConfig>(config.GetSection("Kafka"));
        bus.WithRetry(o => o.MaxAttempts = 5);   // policies are per bus now
    });
});
```

## Mechanical rules

| 1.x | 2.0 |
| --- | --- |
| `m.Add<Broker>(o => ...)` / `m.Add<Broker>(section)` | `m.AddBus(bus => bus.AddTransport(new <Broker>Config { ... }))` / `bus.AddTransport<<Broker>Config>(section)` |
| `<Broker>Options` | `<Broker>Config` (same properties, same validation attributes) |
| `builder.Configure(o => ...)` inside `Add<Broker>` | Properties set directly on the config instance |
| Second transport in the same `AddRunaxMessaging` | A second `AddBus("name", ...)` — one bus wraps exactly one transport, a second `AddTransport` on the same bus throws |
| `m.PublishTo("rabbitmq")` | Deleted — a bus always publishes to its only transport; the old `PublishTo` target becomes your default bus |
| `IMessagePublisher` | `IBus` — identical publish method signatures, so call sites don't change; only the injected type does |
| `IMessagePublisherFactory.ForTransport("kafka")` | Inject the bus wrapping that broker: `[FromKeyedServices("kafka")] IBus`, or `IBusProvider.GetBus("kafka")` |
| `m.AddConsumer<T>()` (all transports) / `m.AddConsumer<T>("sqs")` | `bus.AddConsumer<T>()` on each bus that should deliver to it |
| Global `WithRetry` / `UseSerializer` / `OnUnroutableMessage` / `ConfigureSerialization` (+ `*ForTopic`) | The same methods on each `BusBuilder` — there is no global scope; share defaults via a helper `Action<BusBuilder>` applied to each bus |
| `healthChecks.Add<Broker>Transport()` | Deleted — a health check named `runax:{bus}` auto-registers per bus (`RegisterHealthCheck = false` on the config to opt out) |
| `m.AddOutbox()` + `m.AddInMemoryOutboxStore()` | `bus.AddOutbox()` + `bus.AddOutboxStore(new InMemoryOutboxStoreConfig())` — both on the same bus, validated as a pair |
| `harness.ConfigureMessaging(m => m.WithRetry(...))` (TestKit) | `harness.ConfigureBus(bus => bus.WithRetry(...))`; extra buses via `WithBus("name", ...)` |

## Behavioral changes to be aware of

- **Transport config validation moved earlier.** DataAnnotations on a config now validate when
  the `AddBus` block completes (an `InvalidOperationException` at startup), not via
  `OptionsValidationException` when the host starts.
- **Modes are new.** A bus can declare `bus.Mode = BusMode.PublishOnly` (consumer registrations
  throw at configure time; no consumer hosted service is started) or `BusMode.ConsumeOnly`
  (publishing on the bus throws). The mode governs the application surface only — the consume
  pipeline's dead-letter publish still works on a `ConsumeOnly` bus; use
  `DeadLetterStrategy.BrokerNative` if your credentials can't write.
- **Custom `IOutboxStore` implementations** must add the `bus` parameter to `GetPendingAsync`
  and persist the new `OutboxMessage.Bus` field.
- **Telemetry** gains a `messaging.runax.bus` tag on every span and metric;
  `messaging.system` and `messaging.destination.name` are unchanged.
- **Consumers registered on several buses** receive each bus's traffic through that bus's own
  retry/serialization pipeline; the consumer instance is still a shared singleton — keep
  consumers stateless.

## Custom transports

Instead of shipping an `Add<Broker>` extension method, derive a config type:

```csharp
public sealed class MyBrokerConfig : TransportConfig
{
    [Required] public string? Endpoint { get; set; }

    public override string SystemName => "my-broker";

    protected override IMessagingTransport CreateTransport(TransportContext context) =>
        new MyBrokerTransport(this, context.Services.GetRequiredService<ILogger<MyBrokerTransport>>());
}
```

`TransportContext` carries the bus name and its `BusMode`, so the transport can skip building
producer resources on a `ConsumeOnly` bus (and subscription resources on `PublishOnly`). See
[writing a custom transport](writing-a-custom-transport.md).
