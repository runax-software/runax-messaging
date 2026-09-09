# Runax.Messaging.Outbox

Transactional outbox for [Runax.Messaging](https://github.com/runax-software/runax-messaging).
Persist messages in the **same database transaction** as your business data, then let a background
dispatcher deliver them to the transport — so a crash between "commit" and "publish" can't lose a message.

## Install

```bash
dotnet add package Runax.Messaging
dotnet add package Runax.Messaging.Outbox
```

## Register

An outbox belongs to **one bus** — configure both halves inside that bus's `AddBus` block:

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
        bus.AddOutboxStore(new InMemoryOutboxStoreConfig());   // or your own OutboxStoreConfig
    });
});
```

`AddOutbox` swaps the bus's publish sink: `bus.PublishAsync` writes the envelope to the
`IOutboxStore` instead of the transport, and the per-bus `OutboxDispatcher` background service
drains pending entries to the bus's transport and marks them dispatched. Buses without an outbox
publish straight to their transport, so publishing on another (outbox-less) bus skips the outbox.
`AddOutbox` on a `BusMode.ConsumeOnly` bus throws at configuration time — the outbox exists to
publish.

## Providing a durable store

`AddOutbox` registers the *pattern* only — it does **not** register a store. You must pair it with
`AddOutboxStore(...)`: an outbox without a store (or a store without an outbox) throws when the
`AddBus` block completes, and a second `AddOutboxStore` on the same bus throws like a second
`AddTransport` does. Stores register via a config type — the same uniform pattern as
`AddTransport`: `InMemoryOutboxStoreConfig` ships in the box (tests/single-process only), and a
durable store (EF Core, Dapper, Mongo, ADO.NET, …) derives `OutboxStoreConfig`:

```csharp
public sealed class EfOutboxStoreConfig : OutboxStoreConfig
{
    // protected (not protected internal): the base member's `internal` half
    // doesn't carry across assemblies.
    protected override IOutboxStore CreateStore(OutboxStoreContext context) =>
        new EfOutboxStore(context.Services.GetRequiredService<IDbContextFactory<AppDbContext>>());
}
```

The atomicity guarantee comes from your store: implement `IOutboxStore` so that `AddAsync` **enlists in
the caller's transaction** (e.g. adds a row to your EF Core `DbContext` without calling `SaveChanges`),
so the outbox row commits together with your business data. Note that `GetPendingAsync` takes the
**bus name** and `OutboxMessage` carries a `Bus` field, so one store (one table) can serve several
buses:

```csharp
public sealed class EfOutboxStore(AppDbContext db) : IOutboxStore
{
    public Task AddAsync(OutboxMessage message, CancellationToken ct = default)
    {
        db.OutboxMessages.Add(message);   // committed by the caller's SaveChangesAsync
        return Task.CompletedTask;
    }

    public async Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(
        string bus, int maxCount, CancellationToken ct = default) =>
        await db.OutboxMessages.Where(m => m.Bus == bus && m.DispatchedAt == null)
            .OrderBy(m => m.CreatedAt).Take(maxCount).ToListAsync(ct);

    public async Task MarkDispatchedAsync(Guid id, CancellationToken ct = default) =>
        await db.OutboxMessages.Where(m => m.Id == id)
            .ExecuteUpdateAsync(s => s.SetProperty(m => m.DispatchedAt, DateTimeOffset.UtcNow), ct);
}
```

`InMemoryOutboxStore` is provided for tests and single-process use only — it is not durable or transactional.

> **Scoping.** The bus's publish sink and the `OutboxDispatcher` are singletons, so a store that
> depends on a scoped `DbContext` should not capture it directly. Resolve the unit of work per
> operation instead — inject `IDbContextFactory<AppDbContext>` (or `IServiceScopeFactory`) and
> create a context inside each `IOutboxStore` call.

## Options

Passed to `bus.AddOutbox(o => ...)` (`OutboxOptions`):

| Option | Default | Description |
| --- | --- | --- |
| `PollingInterval` | `5s` | How often the dispatcher polls the store. |
| `BatchSize` | `100` | Maximum pending messages drained per poll. |

## License

MIT
