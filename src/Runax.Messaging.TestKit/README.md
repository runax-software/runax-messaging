# Runax.Messaging.TestKit

Test-support for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Drive your consumers **without a running broker**: the `MessagingTestHarness` spins up a real
dependency-injection container and hosted dispatch pipeline over a default bus with a recording
in-memory transport, so a test can publish a message and then assert what a consumer received —
how many times, and whether it was retried or dead-lettered.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.TestKit
```

## Consumer under test

The same consumer you register in production — no test-only base class:

```csharp
using Runax.Messaging;

public sealed record OrderPlaced(int Id);

public sealed class OrderPlacedConsumer(OrderStore store) : MessageConsumer<OrderPlaced>
{
    public override string Topic => "orders.placed";

    protected override ValueTask HandleAsync(OrderPlaced order, CancellationToken cancellationToken)
    {
        store.Add(order.Id);
        return ValueTask.CompletedTask;
    }
}
```

In production you would wire it up with the usual nested block:

```csharp
builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new InMemoryConfig());
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

## Publish and assert with the harness

The harness registers your consumers and their dependencies, starts dispatch over the recording
transport, lets you publish, and awaits the result:

```csharp
using Runax.Messaging.TestKit;

[Fact]
public async Task Consumer_handles_the_order()
{
    var store = new OrderStore();

    await using var harness = await MessagingTestHarness.Create()
        .AddService(store)                     // dependency the consumer needs
        .AddConsumer<OrderPlacedConsumer>()    // consumer under test, on the default bus
        .StartAsync();

    await harness.PublishAsync("orders.placed", new OrderPlaced(42));

    // Wait until the message is delivered and handled, then assert.
    var order = await harness.WaitForConsumedAsync<OrderPlaced>("orders.placed");

    order.Id.ShouldBe(42);
    store.Handled.ShouldBe([42]);
}
```

`Create()` returns a fluent builder; `StartAsync()` returns a running `MessagingTestHarness`. Dispose it with
`await using` to stop the host.

## Assert retries and dead-lettering

The harness observes every delivery, so you can assert the reliability pipeline too. Tune it via
`ConfigureBus(...)`, which exposes the same `BusBuilder` used in production (`WithRetry`,
`OnUnroutableMessage`, and so on) for the harness's default bus — the recording transport is
already registered, so don't add another:

```csharp
await using var harness = await MessagingTestHarness.Create()
    .AddConsumer<AlwaysFailsConsumer>()
    .ConfigureBus(bus => bus.WithRetry(o => o.MaxAttempts = 2))
    .StartAsync();

await harness.PublishAsync("orders.placed", new OrderPlaced(99));

// A message that exhausts its retries is republished on "<topic>.dead-letter".
var deadLettered = await harness.WaitForDeadLetterAsync("orders.placed");

deadLettered.As<OrderPlaced>()!.Id.ShouldBe(99);
```

## Multi-bus tests

Add extra recording buses with `WithBus(...)` and publish on them with `PublishOnBusAsync(...)` —
for example to assert that a consumer registered on two buses receives each bus's traffic:

```csharp
await using var harness = await MessagingTestHarness.Create()
    .AddConsumer<OrderPlacedConsumer>()                        // default bus
    .WithBus("audit", bus => bus.AddConsumer<AuditConsumer>()) // extra recording bus
    .StartAsync();

await harness.PublishOnBusAsync("audit", "audit.entry", new AuditEntry(7));
```

Every delivery lands in the shared `harness.Delivered` list; `RecordedMessage.Bus` says which bus
it was observed on.

## API at a glance

| Member | Purpose |
| --- | --- |
| `MessagingTestHarness.Create()` | Start a fluent builder. |
| `.AddConsumer<TConsumer>()` | Register a consumer under test on the harness's default bus. |
| `.AddService<TService>(instance)` / `.AddService<TService, TImpl>()` | Register a dependency (real, fake, or NSubstitute mock). |
| `.ConfigureServices(...)` / `.ConfigureBus(...)` | Escape hatches for arbitrary DI or default-bus configuration. |
| `.WithBus(name, configure)` | Add an extra named recording bus for multi-bus scenarios. |
| `.StartAsync()` | Build the container, start dispatch, return a running harness. |
| `harness.PublishAsync(topic, message)` | Publish on the default bus, exactly as application code publishing through `IBus` would. |
| `harness.PublishOnBusAsync(bus, topic, message)` | Publish on a named harness bus. |
| `harness.WaitForConsumedAsync(topic)` / `<TMessage>(topic)` | Await a delivery that a consumer handled; the typed overload returns the payload. |
| `harness.WaitForDeadLetterAsync(topic)` | Await a framework-dead-lettered message on `<topic>.dead-letter`. |
| `harness.ConsumedCount(topic)` / `harness.Delivered` | Inspect how many, and exactly which, messages were observed (`RecordedMessage.Bus` names the bus). |
| `harness.Services` | Resolve consumers, dependencies, or an `IBus` directly (unkeyed = default bus, keyed by name for extra buses). |

Every `WaitFor…` method takes an optional `timeout` (default 5 seconds) and throws `TimeoutException` if the
expected message never arrives.

## License

MIT
