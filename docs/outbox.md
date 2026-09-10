# Transactional outbox

The optional `Runax.Messaging.Outbox` package makes "save data + publish
message" atomic: messages are persisted in the **same database transaction** as
your business data, then delivered to the broker by a background dispatcher —
so a crash between "commit" and "publish" can't lose (or falsely announce) a
message.

```bash
dotnet add package Runax.Messaging.Outbox
```

An outbox belongs to **one bus** (see [Buses](buses.md)): it intercepts that
bus's publishes and delivers them through that bus's transport; buses without
an outbox are unchanged.

## Why an outbox

The problem it solves is the **dual write**. A typical handler does two things:

```csharp
db.Orders.Add(order);
await db.SaveChangesAsync(cancellationToken);              // write #1: the database
await bus.PublishAsync("orders.placed", evt, cancellationToken); // write #2: the broker
```

The database and the broker are separate systems with no shared transaction, so
whichever order you pick, there is a crash window between the two writes:

- **Save, then publish.** The process dies after `SaveChangesAsync` — the order
  exists but `orders.placed` is never announced. Nothing errors; downstream
  services just silently diverge.
- **Publish, then save.** The process dies after `PublishAsync` — consumers
  react to an order that was never committed. Rolling back the database does not
  unpublish the message.

The outbox pattern removes the dual write by making both writes go to **one**
system — your database:

1. **Persist in the business transaction.** Publishing writes the serialized
   message to an *outbox table*, enlisted in the same transaction as the
   business rows. One commit; both or neither.
2. **Dispatch asynchronously.** A background dispatcher polls the table,
   delivers pending rows to the broker, and marks each one dispatched.

### What it gives you — and what it doesn't

| Guarantee | Outcome |
| --- | --- |
| No lost messages | ✔ A committed business change always has its message row; the dispatcher retries delivery until the broker accepts it (**at-least-once**). |
| No phantom messages | ✔ A rolled-back transaction rolls the message row back with it — nothing reaches the broker. |
| Exactly-once delivery | ✘ A crash after the broker accepted a message but before it was marked dispatched re-delivers it on restart. **Consumers must be idempotent.** |
| Strict ordering | ✘ Rows are drained oldest-first per bus, which preserves order in the happy path — but retries, duplicates, and broker semantics mean you should not depend on it. |
| Synchronous delivery | ✘ Delivery lags publish by up to the polling interval. `PublishAsync` returning means *durably enqueued*, not *on the broker*. |

The consume-side complement — an inbox that deduplicates deliveries — is on
the [roadmap](../ROADMAP.md) ("Inbox / idempotent consumer"). Until then,
idempotency is your consumer's job; see
[Operational guidance](#operational-guidance).

## Wiring it up

An outbox has two halves, both configured inside the bus's `AddBus` block, in
either order: `AddOutbox()` turns the pattern on, and `AddOutboxStore(...)`
registers where the rows live. **Both must be present** — either alone is a
configuration error, validated when the block completes.

```csharp
using Runax.Messaging;
using Runax.Messaging.Outbox;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new RabbitMqConfig { HostName = "localhost" });
        bus.AddConsumer<OrderPlacedConsumer>();

        bus.AddOutbox(o => o.PollingInterval = TimeSpan.FromSeconds(2));
        bus.AddOutboxStore(new InMemoryOutboxStoreConfig()); // or your durable store
    });
});
```

### Registering the store

Stores register via a config type derived from `OutboxStoreConfig` — the same
uniform pattern as `AddTransport` (see [Buses](buses.md)). Three overloads:

```csharp
// 1. A pre-built config instance.
bus.AddOutboxStore(new InMemoryOutboxStoreConfig());
// 2. Create the config type and configure it inline.
bus.AddOutboxStore<MyOutboxStoreConfig>(c => c.ConnectionString = "...");
// 3. Create the config type and bind it from a configuration section.
bus.AddOutboxStore<MyOutboxStoreConfig>(builder.Configuration.GetSection("Messaging:Outbox"));
```

`MyOutboxStoreConfig` stands for any `OutboxStoreConfig`-derived type with a
parameterless constructor (overloads 2 and 3 require `new()`); the package
ships `InMemoryOutboxStoreConfig` (tests and single-process use only), and
[Writing a real store](#writing-a-real-store) builds a durable one.

### Options

`OutboxOptions`, passed to `bus.AddOutbox(o => ...)`:

| Option | Default | Description |
| --- | --- | --- |
| `PollingInterval` | 5 seconds | How often the dispatcher polls the store for pending messages. |
| `BatchSize` | `100` | Maximum pending messages drained per poll. Must be ≥ 1. |

Options are registered as **named options keyed by the bus name**, so two
outbox buses tune independently. They are DataAnnotations-validated at startup
(`ValidateOnStart`), and the dispatcher reads them once when it starts —
changing them requires a restart.

### Configure-time validation

Misconfiguration fails fast — at the end of the `AddBus` block (or immediately,
for double registration), not at first publish:

| Mistake | `InvalidOperationException` message |
| --- | --- |
| `AddOutbox` without any store | `Bus '<name>' has an outbox but no store. Register one with bus.AddOutboxStore(...) (e.g. bus.AddOutboxStore(new InMemoryOutboxStoreConfig())).` |
| `AddOutboxStore` without `AddOutbox` | `Bus '<name>' has an outbox store but no outbox. Add bus.AddOutbox() alongside the store.` |
| Outbox on a `BusMode.ConsumeOnly` bus | `Bus '<name>' is ConsumeOnly, but an outbox was configured on it — the outbox exists to publish.` |
| `AddOutbox` twice on one bus | `Bus '<name>' already has an outbox configured.` |
| `AddOutboxStore` twice on one bus | `Bus '<name>' already has an outbox store. A bus has exactly one store; point several buses at the same database via each bus's own store config instead.` |

## How it works inside

### The publish sink swap

A bus's publish pipeline is *mode guard → serialize → sink* (see
[Architecture](architecture.md)). The sink — the bus's transport by default —
is where the envelope leaves the pipeline; `AddOutbox` registers a replacement
sink (keyed to the bus) that writes each envelope to the `IOutboxStore`:

```
bus.PublishAsync(topic, message)
        └─ mode guard, producer span, trace-context injection   (unchanged)
                └─ serialize → envelope JSON                    (unchanged)
                        └─ sink: store.AddAsync(new OutboxMessage { Bus, Topic, Payload })
                           (instead of IMessagingTransport.PublishAsync)
```

Everything the caller sees is unchanged: same `IBus`, same `PublishAsync` /
`PublishBatchAsync` (see [Publishing](publishing.md)), same `ConsumeOnly`
guard, serializer resolution, and telemetry. Consequences worth knowing:

- **The envelope is serialized once, at publish time.** The stored `Payload` is
  the final envelope JSON — headers, contract version, and W3C trace context
  are captured when you publish, so the eventual consumer span still links to
  the producer span even though delivery is asynchronous.
- **A batch publish becomes one row per message** — `PublishBatchAsync` writes
  its envelopes to the store individually.
- **The `runax.messaging.published` counter counts store writes** on an outbox
  bus — durably-enqueued messages, not broker deliveries.

### The dispatcher loop

`AddOutbox` also registers an `OutboxDispatcher` — a hosted `BackgroundService`,
**one per outbox bus**, over that bus's own store and transport:

```
every PollingInterval:
    pending = store.GetPendingAsync(busName, BatchSize)      // oldest first
    foreach message in pending:
        transport.PublishAsync(message.Topic, message.Payload)
        store.MarkDispatchedAsync(message.Id)
```

- **"Dispatched" means the transport accepted the publish** — the broker has
  the message. It says nothing about consumption; consume-side retries and
  dead-lettering are a separate concern ([Architecture](architecture.md)).
- **Failure leaves the row pending.** Any exception — store unreachable, broker
  down, one publish failing — is logged, the rest of the batch is abandoned,
  and every row not yet marked dispatched is retried on the next poll. There is
  no retry cap and no outbox-side dead-letter: the dispatcher retries forever,
  which is what you want when the broker is down for an hour.
- The dispatcher is a hosted service, so draining requires a running .NET
  Generic Host — same as consuming.

### Crash windows and at-least-once

The pattern trades the dual-write's *lost messages* for *possible duplicates*:
a crash **after commit, before dispatch** just delays delivery — the row stays
pending and the restarted dispatcher delivers it. But a crash **between
`transport.PublishAsync` and `MarkDispatchedAsync`** re-publishes a message the
broker already has. That window is small but irreducible; it is why delivery is
**at-least-once** and consumers must tolerate duplicates.

## Writing a real store

`InMemoryOutboxStore` commits immediately on `AddAsync` — plumbing without the
atomicity. The guarantee comes from *your* store; the contract is three methods:

| Member | Contract |
| --- | --- |
| `Task AddAsync(OutboxMessage message, CancellationToken ct)` | Persist a message. **Must enlist in the caller's current unit of work / transaction rather than committing on its own** — that atomic write is the whole point of the pattern. Called by the publish sink, inside your application's transaction. |
| `Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(string bus, int maxCount, CancellationToken ct)` | Return up to `maxCount` messages for the given bus where `DispatchedAt` is null, **oldest first** (`CreatedAt` ascending). Called by the dispatcher each poll. |
| `Task MarkDispatchedAsync(Guid id, CancellationToken ct)` | Record that the message was delivered (set `DispatchedAt`) so it is never returned by `GetPendingAsync` again. |

The row shape is `OutboxMessage`: `Id` (`Guid`, the key), `Bus` (which bus
published it — see [Multi-bus stores](#multi-bus-stores)), `Topic`, `Payload`
(the serialized envelope), `CreatedAt`, and nullable `DispatchedAt`.

> **Lifetime.** The publish sink and dispatcher are singletons and the store is
> created once per bus — so never capture a scoped `DbContext` in the store's
> constructor. Resolve the unit of work per operation, as the example does.

### Worked example: EF Core

A complete store over EF Core (assumes your provider's
`Microsoft.EntityFrameworkCore` packages). Map `OutboxMessage` itself as an
entity — no parallel DTO needed:

```csharp
using Microsoft.EntityFrameworkCore;
using Runax.Messaging.Outbox;

public sealed class Order
{
    public int Id { get; set; }
    public required string Product { get; set; }
}

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxMessage>(outbox =>
        {
            outbox.ToTable("outbox_messages");
            outbox.HasKey(m => m.Id);

            // The dispatcher's hot query: pending rows for one bus, oldest first.
            // The filter syntax is provider-specific (shown: SQL Server).
            outbox.HasIndex(m => new { m.Bus, m.CreatedAt })
                .HasFilter("[DispatchedAt] IS NULL");
        });
    }
}
```

The store creates a short-lived context per call via `IDbContextFactory`:

```csharp
public sealed class EfOutboxStore(IDbContextFactory<AppDbContext> contextFactory) : IOutboxStore
{
    public async Task AddAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        // Enlists in the caller's ambient TransactionScope (see below):
        // the outbox row commits with the business data — or not at all.
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        db.OutboxMessages.Add(message);
        await db.SaveChangesAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(
        string bus, int maxCount, CancellationToken cancellationToken = default)
    {
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        return await db.OutboxMessages.AsNoTracking()
            .Where(m => m.Bus == bus && m.DispatchedAt == null)
            .OrderBy(m => m.CreatedAt).Take(maxCount)
            .ToListAsync(cancellationToken);
    }

    public async Task MarkDispatchedAsync(Guid id, CancellationToken cancellationToken = default)
    {
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        await db.OutboxMessages.Where(m => m.Id == id).ExecuteUpdateAsync(
            s => s.SetProperty(m => m.DispatchedAt, DateTimeOffset.UtcNow), cancellationToken);
    }
}
```

The config type attaches it to a bus:

```csharp
public sealed class EfOutboxStoreConfig : OutboxStoreConfig
{
    // `protected` (not `protected internal`): the base member's `internal`
    // half does not carry across assemblies.
    protected override IOutboxStore CreateStore(OutboxStoreContext context) =>
        new EfOutboxStore(context.Services.GetRequiredService<IDbContextFactory<AppDbContext>>());
}
```

Register the factory with `builder.Services.AddDbContextFactory<AppDbContext>(...)`,
then `bus.AddOutboxStore(new EfOutboxStoreConfig())` alongside `bus.AddOutbox()`.

Application code then wraps the business write and the publish in one
transaction. With the factory-per-call store above, the shared transaction is an
ambient `System.Transactions.TransactionScope` (supported by SQL Server and
Npgsql, among others):

```csharp
using System.Transactions;
using Runax.Messaging.Abstractions;

public sealed record OrderPlaced(int Id);

public sealed class PlaceOrderHandler(AppDbContext db, IBus bus)
{
    public async Task PlaceAsync(string product, CancellationToken cancellationToken)
    {
        using var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        var order = new Order { Product = product };
        db.Orders.Add(order);
        await db.SaveChangesAsync(cancellationToken);

        // Not a network call: writes the envelope to outbox_messages,
        // enlisted in the same ambient transaction as the order row.
        await bus.PublishAsync("orders.placed", new OrderPlaced(order.Id), cancellationToken);

        transaction.Complete(); // both rows commit together
    }
}
```

Throw before `Complete()` and both rows roll back — no phantom message.

> **Shared-context variant.** If you can hand the store the caller's own
> `DbContext`, `AddAsync` can simply `db.OutboxMessages.Add(message)`
> **without** calling `SaveChanges` — the caller's single `SaveChangesAsync`
> then commits business row and outbox row atomically, no `TransactionScope`
> (the [package README](../src/Runax.Messaging.Outbox/README.md) sketches this
> shape). The catch: the store is a singleton, so bridging it to the current
> scope's context is on you.

### Indexing and cleanup

- **Index the dispatcher's query.** `GetPendingAsync` filters
  `Bus = @bus AND DispatchedAt IS NULL` ordered by `CreatedAt` — give it a
  (filtered, where supported) index on `(Bus, CreatedAt)` as in the mapping
  above, or the poll becomes a table scan as history accumulates.
- **Purge dispatched rows.** `MarkDispatchedAsync` updates rather than deletes —
  a useful audit trail — but rows accumulate forever unless you clean up. Run a
  periodic job:

  ```csharp
  await db.OutboxMessages
      .Where(m => m.DispatchedAt != null && m.DispatchedAt < DateTimeOffset.UtcNow.AddDays(-7))
      .ExecuteDeleteAsync(cancellationToken);
  ```

  Deleting inside `MarkDispatchedAsync` is equally valid — the contract only
  requires that dispatched rows never come back from `GetPendingAsync`.

## Multi-bus stores

Every `OutboxMessage` carries the **`Bus`** it was published on, and
`GetPendingAsync` takes the bus name — so one store, one table, several buses:

```csharp
messaging.AddBus("orders", bus =>
{
    bus.AddTransport(new RabbitMqConfig { HostName = "rabbit" });
    bus.AddOutbox();
    bus.AddOutboxStore(new EfOutboxStoreConfig());
});
messaging.AddBus("analytics", bus =>
{
    bus.AddTransport(new SqsConfig { Region = "eu-central-1" });
    bus.AddOutbox();
    bus.AddOutboxStore(new EfOutboxStoreConfig());   // same table, own config
});
```

Each bus still registers its **own** store config (a second `AddOutboxStore` on
the same bus throws), but both configs can point at the same database and
table. Each bus's dispatcher polls with its own bus name, so it drains only its
own rows through its own transport: `orders` rows go to RabbitMQ, `analytics`
rows to SQS, from one `outbox_messages` table — which is why the
`(Bus, CreatedAt)` index leads with `Bus`.

## Operational guidance

- **Tune `PollingInterval` for latency, `BatchSize` for throughput.** Worst-case
  added latency is roughly one polling interval; sustained throughput per bus
  is capped near `BatchSize / PollingInterval`. The defaults (5 s / 100) suit
  background workflows; drop the interval for latency-sensitive events, and
  remember each poll is a database query. Options are per bus — tune each
  independently.
- **Monitor pending depth and age**: `COUNT(*) WHERE Bus = @bus AND
  DispatchedAt IS NULL`, and the age of the oldest pending row. Growing depth
  means the dispatcher is falling behind or the broker is down (each failed
  cycle is logged); an old head with small depth points at a poison row the
  transport keeps rejecting. The bus's broker health check (`runax:{bus}`)
  covers the transport side.
- **Make consumers idempotent.** At-least-once delivery is a property of the
  pattern, not a bug to configure away. Deduplicate on a natural business key
  or record handled message IDs — until the roadmap's
  [inbox / idempotent consumer](../ROADMAP.md) lands, this is the consuming
  service's job and the outbox's essential partner pattern.
- **Testing.** Pair the outbox with
  `bus.AddOutboxStore(new InMemoryOutboxStoreConfig())` — publishes land in the
  in-memory store and the dispatcher drains them to the (test) transport with
  no database. The dispatcher only runs while a host does, and delivery lags
  publish by up to the polling interval — shorten `PollingInterval` in tests
  and await the outcome rather than asserting immediately. See
  [Testing](testing.md) for the TestKit harness.

## See also

- [Buses](buses.md) — bus blocks, modes, and the config-type pattern the store registration follows.
- [Publishing](publishing.md) — the `IBus` publish API the outbox sits behind.
- [Testing](testing.md) — the TestKit harness and testing outbox-enabled buses.
- [Architecture & message flow](architecture.md) — where the sink sits in the publish pipeline.
- [Package README](../src/Runax.Messaging.Outbox/README.md) — the short version of this guide.
