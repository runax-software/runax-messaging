using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Outbox;

/// <summary>
/// Bus extensions for the transactional outbox. An outbox belongs to one bus:
/// <c>bus.AddOutbox()</c> routes the bus's publishes into the registered store, and
/// <c>bus.AddOutboxStore(...)</c> registers that store via a config type — the same uniform
/// pattern as <c>bus.AddTransport(...)</c>. Both must be present; the pairing is validated when
/// the <c>AddBus</c> block completes.
/// </summary>
public static class OutboxBusBuilderExtensions
{
    private static readonly object OutboxEnabledKey = new();
    private static readonly object StoreConfigKey = new();

    /// <summary>
    /// Routes this bus's publishes through the outbox: envelopes are written to the registered
    /// <see cref="IOutboxStore"/> instead of the transport, and a background dispatcher delivers
    /// them to the transport. Register a store with
    /// <see cref="AddOutboxStore(BusBuilder, OutboxStoreConfig)"/>.
    /// </summary>
    /// <param name="bus">The bus builder.</param>
    /// <param name="configure">Optional action to configure <see cref="OutboxOptions"/>.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown at the end of the <c>AddBus</c> block when the bus is
    /// <see cref="BusMode.ConsumeOnly"/> or no store was registered.
    /// </exception>
    public static BusBuilder AddOutbox(this BusBuilder bus, Action<OutboxOptions>? configure = null)
    {
        if (bus.Properties.ContainsKey(OutboxEnabledKey))
            throw new InvalidOperationException($"Bus '{bus.BusName}' already has an outbox configured.");

        bus.Properties[OutboxEnabledKey] = true;

        var options = bus.Services
            .AddOptions<OutboxOptions>(bus.BusName)
            .ValidateDataAnnotations()
            .ValidateOnStart();
        if (configure is not null) options.Configure(configure);

        var busName = bus.BusName;

        // Swap the bus's publish sink: envelopes land in the store instead of the transport.
        bus.Services.AddKeyedSingleton<IBusPublishSink>(busName, (sp, _) =>
            new OutboxSink(busName, sp.GetRequiredKeyedService<IOutboxStore>(busName)));

        bus.Services.AddSingleton<IHostedService>(sp =>
            ActivatorUtilities.CreateInstance<OutboxDispatcher>(sp, busName));

        bus.OnValidate(static b =>
        {
            if (b.Mode == BusMode.ConsumeOnly)
            {
                throw new InvalidOperationException(
                    $"Bus '{b.BusName}' is ConsumeOnly, but an outbox was configured on it — the outbox exists to publish.");
            }

            if (!b.Properties.ContainsKey(StoreConfigKey))
            {
                throw new InvalidOperationException(
                    $"Bus '{b.BusName}' has an outbox but no store. Register one with " +
                    "bus.AddOutboxStore(...) (e.g. bus.AddOutboxStore(new InMemoryOutboxStoreConfig())).");
            }
        });

        return bus;
    }

    /// <summary>
    /// Registers this bus's outbox store from a pre-built config instance. One store per bus.
    /// </summary>
    /// <param name="bus">The bus builder.</param>
    /// <param name="config">The store configuration.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    /// <exception cref="InvalidOperationException">The bus already has an outbox store.</exception>
    public static BusBuilder AddOutboxStore(this BusBuilder bus, OutboxStoreConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        if (bus.Properties.ContainsKey(StoreConfigKey))
        {
            throw new InvalidOperationException(
                $"Bus '{bus.BusName}' already has an outbox store. A bus has exactly one store; " +
                "point several buses at the same database via each bus's own store config instead.");
        }

        bus.Properties[StoreConfigKey] = config;

        var busName = bus.BusName;
        bus.Services.AddKeyedSingleton<IOutboxStore>(busName, (sp, _) =>
            config.CreateStore(new OutboxStoreContext(sp, busName)));
        config.ConfigureServices(bus.Services, busName);

        bus.OnValidate(static b =>
        {
            if (!b.Properties.ContainsKey(OutboxEnabledKey))
            {
                throw new InvalidOperationException(
                    $"Bus '{b.BusName}' has an outbox store but no outbox. Add bus.AddOutbox() alongside the store.");
            }
        });

        return bus;
    }

    /// <summary>
    /// Creates the store config type and applies the delegate:
    /// <c>bus.AddOutboxStore&lt;MyStoreConfig&gt;(c =&gt; c.ConnectionString = ...)</c>.
    /// </summary>
    /// <typeparam name="TConfig">The store config type.</typeparam>
    /// <param name="bus">The bus builder.</param>
    /// <param name="configure">Action that configures the new config instance.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static BusBuilder AddOutboxStore<TConfig>(this BusBuilder bus, Action<TConfig> configure)
        where TConfig : OutboxStoreConfig, new()
    {
        ArgumentNullException.ThrowIfNull(configure);

        var config = new TConfig();
        configure(config);
        return AddOutboxStore(bus, config);
    }

    /// <summary>
    /// Creates the store config type and binds it from a configuration section.
    /// </summary>
    /// <typeparam name="TConfig">The store config type.</typeparam>
    /// <param name="bus">The bus builder.</param>
    /// <param name="section">The configuration section to bind the config from.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static BusBuilder AddOutboxStore<TConfig>(this BusBuilder bus, IConfiguration section)
        where TConfig : OutboxStoreConfig, new()
    {
        ArgumentNullException.ThrowIfNull(section);

        var config = new TConfig();
        section.Bind(config);
        return AddOutboxStore(bus, config);
    }
}
