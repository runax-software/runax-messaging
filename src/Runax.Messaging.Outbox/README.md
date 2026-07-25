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

```csharp
using Runax.Messaging;
using Runax.Messaging.Outbox;

builder.Services.AddRunaxMessaging(runax =>
{
    runax.AddRabbitMq(rabbitmq =>
    {
        rabbitmq.Configure(o => o.HostName = "localhost");
        rabbitmq.AddConsumer<OrderPlacedConsumer>();
    });

    runax.AddOutbox(o => o.PollingInterval = TimeSpan.FromSeconds(2));
    runax.AddInMemoryOutboxStore();   // or register your own IOutboxStore
});
```

`AddOutbox` makes `IMessagePublisher` write to the `IOutboxStore` instead of publishing directly;
the `OutboxDispatcher` background service drains pending entries to the transport and marks them dispatched.
Only the default `IMessagePublisher` is wrapped — publishers obtained from
`IMessagePublisherFactory.ForTransport("<system-name>")` write straight to their transport and skip the outbox.

## Providing a durable store

`AddOutbox` registers the *pattern* only — it does **not** register a store. You must supply one:
call `AddInMemoryOutboxStore()` (tests/single-process only) or register your own `IOutboxStore`
(EF Core, Dapper, Mongo, ADO.NET, …). With no store registered, resolution fails at startup.

The atomicity guarantee comes from your store: implement `IOutboxStore` so that `AddAsync` **enlists in
the caller's transaction** (e.g. adds a row to your EF Core `DbContext` without calling `SaveChanges`),
so the outbox row commits together with your business data.

```csharp
public sealed class EfOutboxStore(AppDbContext db) : IOutboxStore
{
    public Task AddAsync(OutboxMessage message, CancellationToken ct = default)
    {
        db.OutboxMessages.Add(message);   // committed by the caller's SaveChangesAsync
        return Task.CompletedTask;
    }

    public async Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int maxCount, CancellationToken ct = default) =>
        await db.OutboxMessages.Where(m => m.DispatchedAt == null)
            .OrderBy(m => m.CreatedAt).Take(maxCount).ToListAsync(ct);

    public async Task MarkDispatchedAsync(Guid id, CancellationToken ct = default) =>
        await db.OutboxMessages.Where(m => m.Id == id)
            .ExecuteUpdateAsync(s => s.SetProperty(m => m.DispatchedAt, DateTimeOffset.UtcNow), ct);
}
```

`InMemoryOutboxStore` is provided for tests and single-process use only — it is not durable or transactional.

> **Scoping.** `OutboxPublisher` and `OutboxDispatcher` are singletons, so a store that depends on a
> scoped `DbContext` should not capture it directly. Resolve the unit of work per operation instead —
> inject `IDbContextFactory<AppDbContext>` (or `IServiceScopeFactory`) and create a context inside each
> `IOutboxStore` call.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `PollingInterval` | `5s` | How often the dispatcher polls the store. |
| `BatchSize` | `100` | Maximum pending messages drained per poll. |

## License

MIT
