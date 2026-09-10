# Testing

Runax.Messaging is designed so that messaging code is tested at three levels, each
answering a different question:

| Tier | Question it answers | Tooling | Broker needed |
| --- | --- | --- | --- |
| **Consumer behavior** | "Given this message, does my consumer do the right thing — including retries and dead-lettering?" | `Runax.Messaging.TestKit` — the `MessagingTestHarness` | No |
| **Wiring & configuration** | "Is my bus topology valid, and do the right services resolve?" | A plain `ServiceCollection` with `InMemoryConfig` | No |
| **Transport integration** | "Does the transport actually round-trip against the real broker?" | `docker compose` + tests tagged `Category=Integration` | Yes (containerized) |

Most application tests live in the first tier. The second tier is for asserting your
own composition root; the third mirrors how the library tests its transport packages
and is the model to follow for a [custom transport](writing-a-custom-transport.md).

The examples below use xUnit and Shouldly assertions, matching this repository's
own test suites, but nothing in the TestKit depends on either.

## The TestKit harness

```bash
dotnet add package Runax.Messaging.TestKit
```

`MessagingTestHarness` is a broker-free test host. It is **not** a mock: it builds a
real dependency-injection container and starts the real hosted dispatch pipeline —
the same `MessageConsumerHostedService`, serializer, retry policy, and dead-letter
strategy that run in production — over a default bus whose transport is a *recording
in-memory transport*. That transport wraps the built-in `InMemoryTransport` and sits
inside the delivery path, so it observes every delivery (with the
`MessageDisposition` the pipeline returned) and every framework dead-letter publish
without stealing messages from your consumers. What you assert against is what the
real pipeline actually did.

The consumer under test is the one you ship — no test-only base class:

```csharp
public sealed record OrderPlaced(int Id);

public sealed class OrderPlacedConsumer(OrderStore store) : MessageConsumer<OrderPlaced>
{
    public override string Topic => "orders.placed";

    protected override ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken)
    {
        store.Add(message.Id);
        return ValueTask.CompletedTask;
    }
}
```

A complete test walks the harness lifecycle — `Create` → register → `StartAsync` →
publish → wait → assert → dispose:

```csharp
using Runax.Messaging.TestKit;

[Fact]
public async Task Consumer_handles_the_order()
{
    var store = new OrderStore();

    await using var harness = await MessagingTestHarness.Create()
        .AddService(store)                  // dependency the consumer needs
        .AddConsumer<OrderPlacedConsumer>() // consumer under test, on the default bus
        .StartAsync();

    await harness.PublishAsync("orders.placed", new OrderPlaced(42));

    var order = await harness.WaitForConsumedAsync<OrderPlaced>("orders.placed");

    order.Id.ShouldBe(42);
    store.Handled.ShouldBe([42]);
}
```

The builder surface:

- `AddConsumer<TConsumer>()` — registers a consumer on the harness's default bus,
  exactly as `bus.AddConsumer<TConsumer>()` inside an `AddBus` block would.
- `AddService<TService>(instance)` / `AddService<TService, TImplementation>()` —
  registers a singleton dependency (a real object, a fake, or an NSubstitute mock),
  resolvable both by the consumers and later from `harness.Services`.
- `ConfigureServices(...)` — escape hatch for arbitrary DI registrations.
- `ConfigureBus(...)` — configures the default bus with the same `BusBuilder` used
  in production (`WithRetry`, `OnUnroutableMessage`, `UseSerializer`, …). The
  recording transport is already registered — **do not** call `AddTransport` here.
- `WithBus(name, configure)` — adds an extra named recording bus (see
  [Multi-bus tests](#multi-bus-tests)).
- `StartAsync()` — builds the container, starts dispatch, returns a running harness.

On the running harness: `PublishAsync(topic, message[, headers])` publishes on the
default bus through the real `IBus`, exactly as application code would, and
`PublishOnBusAsync(bus, topic, message)` on a named harness bus; the `WaitFor…`
methods await outcomes (next section); `ConsumedCount(topic)` and `Delivered`
inspect everything observed so far; `Services` exposes the host's service provider
(unkeyed `IBus` = default bus, keyed by name for extra buses).

Dispose the harness with `await using` (as above) to stop the host. A harness is
single-use: it cannot be restarted after disposal, so create a fresh one per test.

## Asserting outcomes

### Consumed messages

Publishing hands the message to the transport; dispatch happens asynchronously on
the hosted pipeline. `WaitForConsumedAsync` bridges that gap — it completes when a
message on the topic is delivered *and acknowledged* (a consumer handled it):

```csharp
var recorded = await harness.WaitForConsumedAsync("orders.placed");

recorded.Topic.ShouldBe("orders.placed");
recorded.Disposition.ShouldBe(MessageDisposition.Acknowledge);
```

The typed overload (`WaitForConsumedAsync<TMessage>`, shown in the walkthrough
above) returns the deserialized payload directly and throws if the body cannot be
deserialized to `TMessage`.

Every `WaitFor…` method takes an optional `timeout` (default **5 seconds**) and an
optional `CancellationToken`; if nothing matching arrives in time it throws
`TimeoutException` naming what it was waiting for. Waits are satisfied
retroactively from messages already observed, so there is no race between
publishing and registering the wait — but a second identical wait therefore returns
the same (first) matching message rather than blocking for a new one; use a
dependency that signals completion (see the multi-bus example) when a test must
await *several* deliveries.

### Retries

Retry policy is configured through `ConfigureBus` with the production `WithRetry`
API. Keep the delays tiny so tests stay fast:

```csharp
private sealed class TransientThenSucceedsConsumer : MessageConsumer<OrderPlaced>
{
    public static int Attempts;

    public override string Topic => "orders.placed";

    protected override ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken)
    {
        if (Interlocked.Increment(ref Attempts) < 3)
            throw new InvalidOperationException("transient");

        return ValueTask.CompletedTask;
    }
}

[Fact]
public async Task Transient_failures_are_retried_until_success()
{
    TransientThenSucceedsConsumer.Attempts = 0;
    await using var harness = await MessagingTestHarness.Create()
        .AddConsumer<TransientThenSucceedsConsumer>()
        .ConfigureBus(bus => bus.WithRetry(o =>
        {
            o.MaxAttempts = 5;
            o.InitialDelay = TimeSpan.FromMilliseconds(1);
            o.MaxDelay = TimeSpan.FromMilliseconds(2);
        }))
        .StartAsync();

    await harness.PublishAsync("orders.placed", new OrderPlaced(5));

    var recorded = await harness.WaitForConsumedAsync("orders.placed");

    recorded.As<OrderPlaced>()!.Id.ShouldBe(5);
    TransientThenSucceedsConsumer.Attempts.ShouldBe(3); // two transient failures, then success
    harness.ConsumedCount("orders.placed").ShouldBe(1);
}
```

`ConsumedCount(topic)` counts only *acknowledged* deliveries, so a message retried
twice before succeeding still contributes one. Retries redeliver the same message —
inspect `Delivered` for the raw attempt-by-attempt sequence, where requeued attempts
appear with `MessageDisposition.Requeue`.

### Dead letters

With the default framework-managed dead-letter strategy, a message that exhausts its
retries (or throws `PoisonMessageException`) is republished on
`<topic><suffix>` — the suffix defaults to `.dead-letter`. No consumer subscribes to
that topic, so the harness records the dead-letter *publish* itself, and
`WaitForDeadLetterAsync` awaits it:

```csharp
await using var harness = await MessagingTestHarness.Create()
    .AddConsumer<AlwaysFailsConsumer>()
    .ConfigureBus(bus => bus.WithRetry(o => o.MaxAttempts = 2)) // shrink the delays too, as above
    .StartAsync();

await harness.PublishAsync("orders.placed", new OrderPlaced(99));

var deadLettered = await harness.WaitForDeadLetterAsync("orders.placed");

deadLettered.Topic.ShouldBe("orders.placed.dead-letter");
deadLettered.As<OrderPlaced>()!.Id.ShouldBe(99);
```

If you changed `DeadLetterTopicSuffix` in `WithRetry`, pass the same suffix as the
`deadLetterSuffix` argument. `WaitForDeadLetterAsync` requires the framework-managed
strategy (the harness default); a bus switched to `DeadLetterStrategy.BrokerNative`
never republishes, so assert `MessageDisposition.DeadLetter` in `Delivered` instead.

### Anatomy of a `RecordedMessage`

Everything the harness observed is a `RecordedMessage`:

- `Bus` — the bus it was observed on: `BusNames.Default` for the default bus, or the
  name passed to `WithBus(...)`.
- `Topic` — the delivery topic; dead-lettered messages reappear on the dead-letter
  topic as a separate record.
- `Context` — the decoded `MessageContext`: raw body, headers, and (when the message
  type carries `[MessageContract]`) the contract name and version.
- `Disposition` — what the pipeline decided: `Acknowledge` (handled, or
  framework-dead-lettered), `Requeue` (redelivered for retry), or `DeadLetter`
  (rejected for broker-native dead-lettering; also stamped on recorded dead-letter
  publishes).
- `As<TMessage>()` — deserializes the body with the same serializer it was written
  with.

## Multi-bus tests

Add extra recording buses with `WithBus(...)`; each gets its own recording in-memory
transport, and all deliveries land in the shared `Delivered` list with
`RecordedMessage.Bus` naming the source. Here a single consumer type registered on
two buses is shown to receive each bus's traffic (a `Collector` dependency signals
once both arrive, since a topic-level wait alone cannot distinguish the buses):

```csharp
private sealed class Collector
{
    private readonly TaskCompletionSource _signal = new();
    private int _remaining = 2;

    public Task BothReceived => _signal.Task;
    public void Record() { if (Interlocked.Decrement(ref _remaining) == 0) _signal.TrySetResult(); }
}

[Fact]
public async Task A_consumer_on_two_buses_receives_each_bus_traffic()
{
    var collector = new Collector();

    await using var harness = await MessagingTestHarness.Create()
        .AddService(collector)
        .AddConsumer<PingConsumer>()                              // default bus
        .WithBus("audit", bus => bus.AddConsumer<PingConsumer>()) // extra recording bus
        .StartAsync();

    await harness.PublishAsync("ping", new Ping("from-default"));
    await harness.PublishOnBusAsync("audit", "ping", new Ping("from-audit"));

    await collector.BothReceived.WaitAsync(TimeSpan.FromSeconds(5));

    var buses = harness.Delivered
        .Where(m => m.Topic == "ping" && m.Disposition == MessageDisposition.Acknowledge)
        .Select(m => m.Bus)
        .ToList();

    buses.ShouldBe([BusNames.Default, "audit"], ignoreOrder: true);
}
```

As in production, a consumer registered on several buses is still a single DI
instance, and named buses resolve from `harness.Services` via
`GetRequiredKeyedService<IBus>("audit")` or `IBusProvider.GetBus("audit")`.

## Testing configuration itself

Wiring tests need no harness and no host: build a `ServiceCollection`, add logging,
and call `AddRunaxMessaging` with `InMemoryConfig` as the transport. The bus builder
validates each `AddBus` block as it completes, so topology mistakes surface as
`InvalidOperationException` at configuration time — which makes them easy to assert:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

[Fact]
public void Registering_a_consumer_on_a_PublishOnly_bus_throws()
{
    var services = new ServiceCollection();
    services.AddLogging();

    var ex = Should.Throw<InvalidOperationException>(() =>
        services.AddRunaxMessaging(m => m.AddBus("audit", bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.AddConsumer<PingConsumer>();
            bus.Mode = BusMode.PublishOnly; // order inside the block does not matter
        })));

    ex.Message.ShouldContain("PublishOnly");
}
```

The same pattern catches every configuration-time invariant: a second
`AddTransport` on one bus (one bus = one transport), a bus with no transport, a
duplicate bus name, a `DataAnnotations`-invalid transport config, and consume-side
policies (such as `WithRetry`) on a `PublishOnly` bus. `ConsumeOnly` is enforced
later, at the publish call — build the provider and assert that `PublishAsync`
throws.

Resolution rules are equally testable by building the provider:

```csharp
using var provider = services.BuildServiceProvider();

provider.GetRequiredService<IBus>().Name.ShouldBe(BusNames.Default); // unkeyed = default bus
provider.GetRequiredKeyedService<IBus>("audit").Name.ShouldBe("audit");

var buses = provider.GetRequiredService<IBusProvider>();
buses.GetBus("audit").Name.ShouldBe("audit");
buses.Buses.Select(b => b.Name).ShouldBe([BusNames.Default, "audit"]);
```

When several named buses exist and none is the default, resolving the unkeyed `IBus`
throws with the registered names listed — also worth pinning in a test if your app
relies on it. See [Buses — the core model](buses.md) for the full rules.

## Testing with the outbox

`InMemoryOutboxStoreConfig` (in `Runax.Messaging.Outbox`) gives outbox tests an
in-process, non-durable store. Two useful patterns:

**Assert the store, not the wire.** Without a started host the `OutboxDispatcher`
never runs, so a publish lands only in the store — ideal for asserting that
`bus.PublishAsync` was rerouted to the outbox sink:

```csharp
var services = new ServiceCollection();
services.AddLogging();
services.AddRunaxMessaging(m => m.AddBus(bus => bus
    .AddTransport(new InMemoryConfig())
    .AddOutbox()
    .AddOutboxStore(new InMemoryOutboxStoreConfig())));
using var provider = services.BuildServiceProvider();

await provider.GetRequiredService<IBus>().PublishAsync("orders", new Order(1));

var store = provider.GetRequiredKeyedService<IOutboxStore>(BusNames.Default);
var pending = await store.GetPendingAsync(BusNames.Default, 10);
pending.Count.ShouldBe(1);
pending[0].Topic.ShouldBe("orders");
```

The store is keyed by bus name because an outbox belongs to one bus.

**Test end-to-end dispatch.** Start a host and shrink `PollingInterval` so the
dispatcher drains the store quickly:

```csharp
var builder = Host.CreateApplicationBuilder();
builder.Services.AddSingleton(received); // TaskCompletionSource<Order> the consumer completes
builder.Services.AddRunaxMessaging(m => m.AddBus(bus => bus
    .AddTransport(new InMemoryConfig())
    .AddConsumer<OrderConsumer>()
    .AddOutbox(o => o.PollingInterval = TimeSpan.FromMilliseconds(50))
    .AddOutboxStore(new InMemoryOutboxStoreConfig())));
using var host = builder.Build();
await host.StartAsync();

await host.Services.GetRequiredService<IBus>().PublishAsync("orders", new Order(7));

var order = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
order.Id.ShouldBe(7);
```

After dispatch, `GetPendingAsync` returns empty — dispatched rows are marked so they
are not published again. Dispatching is asynchronous, so poll briefly for the store
to drain rather than asserting immediately. When writing your own durable store,
these same two patterns are the acceptance tests to port. See
[Transactional outbox](outbox.md).

## Integration tests

Transport packages are verified against real (containerized) brokers. The repo's
`compose.yml` starts every dependency locally:

```bash
docker compose up -d
dotnet test -- --filter-trait "Category=Integration"
```

Integration tests are marked with `[Trait("Category", "Integration")]`; the plain
unit run excludes them with `--filter-not-trait "Category=Integration"` (this
repository uses xUnit v3 on Microsoft.Testing.Platform, hence the `--` argument
separator). Each suite reads its endpoint from an environment variable and falls
back to the compose defaults:

| Variable | Default | Backing service in `compose.yml` |
| --- | --- | --- |
| `RABBITMQ_HOST` | `localhost` | RabbitMQ on :5672 |
| `AWS_SERVICE_URL` (+ `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) | `http://localhost:4566` | SQS + SNS emulator |
| `PUBSUB_EMULATOR_HOST` | `localhost:8085` | Google Pub/Sub emulator |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka (KRaft, single node) |
| `VALKEY_HOST` / `REDIS_HOST` | `localhost:6379` / `localhost:6380` | Valkey and Redis (the Redis Streams transport is tested against both engines) |
| `SERVICEBUS_CONNECTION_STRING` | emulator on `localhost:5673` | Azure Service Bus emulator (+ SQL Edge backend) |
| `EVENTHUBS_CONNECTION_STRING` / `EVENTHUBS_BLOB_CONNECTION_STRING` | emulator on `localhost:5674` / Azurite on :10000 | Azure Event Hubs emulator + Azurite checkpoint store |

CI runs the same suites as a per-transport matrix (`.github/workflows/ci.yml`): one
job per transport, with the heavyweight emulators (Pub/Sub, Service Bus, Event Hubs,
Kafka) started only for the job that needs them, and each job filtering to its own
test project plus `--filter-trait "Category=Integration"`. Docs-only changes skip
the matrix entirely.

### Writing one for a custom transport

Model yours on `tests/Runax.Messaging.Transports.RabbitMq.Tests/RabbitMqTransportIntegrationTests.cs`:

- Tag the class `[Trait("Category", "Integration")]` so it is excluded from unit
  runs and pulled in by the integration filter.
- Read the endpoint from an environment variable with a `localhost` compose default,
  and add the broker to your compose file.
- Isolate runs: suffix broker resources (exchanges, queues, topics) with
  `Guid.NewGuid():N` so parallel or repeated runs never collide.
- Build the transport the way production does — a `ServiceCollection` with
  `AddRunaxMessaging`/`AddBus`/`AddTransport(yourConfig)` — then resolve it with
  `provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default)` and
  drive `PublishAsync`/`SubscribeAsync` directly against raw envelope JSON.
- Verify the broker side with the broker's own client (e.g. `BasicGetAsync` for
  RabbitMQ), and poll with short delays in a bounded loop instead of a fixed sleep.
- Cover at minimum: publish → subscribe round-trip, batch publish, concurrent
  publishes, and dead-letter behavior for each `MessageDisposition`.

## See also

- [Buses — the core model](buses.md) — modes, multi-bus patterns, resolution rules.
- [Consuming & reliability](consuming.md) — consumers, retries, dead-lettering, unroutable messages.
- [Transactional outbox](outbox.md) — the pattern, wiring, writing a real store.
- [Configuration & per-bus settings](configuration.md) — everything `ConfigureBus` can set.
- [Writing a custom transport](writing-a-custom-transport.md) — the SPI your integration tests exercise.
