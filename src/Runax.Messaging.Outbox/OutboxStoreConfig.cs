using Microsoft.Extensions.DependencyInjection;

namespace Runax.Messaging.Outbox;

/// <summary>
/// Base class for outbox store configuration — the same uniform config-type pattern as
/// <c>TransportConfig</c>. A store package (EF, Dapper, Mongo, ...) derives one config type
/// carrying its settings and implements <see cref="CreateStore"/>. Applications attach the store
/// to a bus with <c>bus.AddOutboxStore(...)</c>; one store per bus.
/// </summary>
public abstract class OutboxStoreConfig
{
    /// <summary>
    /// Creates the store for this config. Called once per bus at first use.
    /// </summary>
    /// <param name="context">The bus the store is being created for.</param>
    /// <returns>The store instance.</returns>
    protected internal abstract IOutboxStore CreateStore(OutboxStoreContext context);

    /// <summary>
    /// Performs additional service registrations for this store. Called once from
    /// <c>AddOutboxStore</c>. The default implementation does nothing.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="busName">The name of the bus the store belongs to.</param>
    protected internal virtual void ConfigureServices(IServiceCollection services, string busName)
    {
    }
}

/// <summary>
/// Runtime context handed to <see cref="OutboxStoreConfig.CreateStore"/>.
/// </summary>
public sealed class OutboxStoreContext
{
    internal OutboxStoreContext(IServiceProvider services, string busName)
    {
        Services = services;
        BusName = busName;
    }

    /// <summary>Gets the application service provider.</summary>
    public IServiceProvider Services { get; }

    /// <summary>Gets the name of the bus the store belongs to.</summary>
    public string BusName { get; }
}
