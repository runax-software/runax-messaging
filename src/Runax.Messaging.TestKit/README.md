# Runax.Messaging.TestKit

Test-support for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Drive your consumers **without a running broker**: the `MessagingTestHarness` spins up a real
dependency-injection container and hosted dispatch pipeline over the built-in in-memory transport, so a
test can publish a message and then assert what a consumer received — how many times, and whether it was
retried or dead-lettered.

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
builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddInMemory(inMemory =>
    {
        inMemory.AddConsumer<OrderPlacedConsumer>();
    });
});
```

## Publish and assert with the harness

The harness registers your consumers and their dependencies, starts dispatch over the in-memory transport,
lets you publish, and awaits the result:

```csharp
using Runax.Messaging.TestKit;

[Fact]
public async Task Consumer_handles_the_order()
{
    var store = new OrderStore();

    await using var harness = await MessagingTestHarness.Create()
        .AddService(store)                     // dependency the consumer needs
        .AddConsumer<OrderPlacedConsumer>()    // consumer under test
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
`ConfigureMessaging(...)`, which exposes the same `MessagingConfigurator` used in production (`WithRetry`,
`OnUnroutableMessage`, and so on):

```csharp
await using var harness = await MessagingTestHarness.Create()
    .AddConsumer<AlwaysFailsConsumer>()
    .ConfigureMessaging(runax => runax.WithRetry(o => o.MaxAttempts = 2))
    .StartAsync();

await harness.PublishAsync("orders.placed", new OrderPlaced(99));

// A message that exhausts its retries is republished on "<topic>.dead-letter".
var deadLettered = await harness.WaitForDeadLetterAsync("orders.placed");

deadLettered.As<OrderPlaced>()!.Id.ShouldBe(99);
```

## API at a glance

| Member | Purpose |
| --- | --- |
| `MessagingTestHarness.Create()` | Start a fluent builder. |
| `.AddConsumer<TConsumer>()` | Register a consumer under test on the in-memory transport. |
| `.AddService<TService>(instance)` / `.AddService<TService, TImpl>()` | Register a dependency (real, fake, or NSubstitute mock). |
| `.ConfigureServices(...)` / `.ConfigureMessaging(...)` | Escape hatches for arbitrary DI or messaging configuration. |
| `.StartAsync()` | Build the container, start dispatch, return a running harness. |
| `harness.PublishAsync(topic, message)` | Publish through the harness's `IMessagePublisher`. |
| `harness.WaitForConsumedAsync(topic)` / `<TMessage>(topic)` | Await a delivery that a consumer handled; the typed overload returns the payload. |
| `harness.WaitForDeadLetterAsync(topic)` | Await a framework-dead-lettered message on `<topic>.dead-letter`. |
| `harness.ConsumedCount(topic)` / `harness.Delivered` | Inspect how many, and exactly which, messages were observed. |
| `harness.Services` | Resolve consumers, dependencies, or `IMessagePublisher` directly. |

Every `WaitFor…` method takes an optional `timeout` (default 5 seconds) and throws `TimeoutException` if the
expected message never arrives.

## License

MIT
