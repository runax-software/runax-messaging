namespace Runax.Messaging.Outbox;

/// <summary>
/// Store config for the in-process <see cref="InMemoryOutboxStore"/>:
/// <c>bus.AddOutboxStore(new InMemoryOutboxStoreConfig())</c>. Intended for tests and
/// single-process scenarios; it is not durable or transactional.
/// </summary>
public sealed class InMemoryOutboxStoreConfig : OutboxStoreConfig
{
    /// <inheritdoc />
    protected internal override IOutboxStore CreateStore(OutboxStoreContext context) =>
        new InMemoryOutboxStore();
}
