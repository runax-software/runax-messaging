using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Configurator extensions for registering buses. A bus is a named, self-contained messaging
/// context wrapping exactly one transport; register several buses to talk to several brokers.
/// </summary>
public static class MessagingConfiguratorExtensions
{
    /// <summary>
    /// Adds the default bus (<see cref="BusNames.Default"/>) — what an unkeyed <see cref="IBus"/>
    /// injection resolves to.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Block that registers the bus's transport, consumers, and policies.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddBus(
        this MessagingConfigurator configurator,
        Action<BusBuilder> configure) =>
        AddBus(configurator, BusNames.Default, configure);

    /// <summary>
    /// Adds a named bus, resolvable via keyed DI (<c>[FromKeyedServices(name)] IBus</c>) or
    /// <see cref="IBusProvider.GetBus"/>. The bus is validated when <paramref name="configure"/>
    /// returns: it must register exactly one transport, and every registration must be compatible
    /// with the declared <see cref="BusBuilder.Mode"/>.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="name">The bus name. Must be unique within the application.</param>
    /// <param name="configure">Block that registers the bus's transport, consumers, and policies.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// A bus with the same name is already registered, the block registered no transport or more
    /// than one, the transport config fails validation, or a registration conflicts with the mode.
    /// </exception>
    public static MessagingConfigurator AddBus(
        this MessagingConfigurator configurator,
        string name,
        Action<BusBuilder> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(configure);

        var services = configurator.Services;

        var duplicate = services
            .Select(d => d.ImplementationInstance)
            .OfType<BusRegistration>()
            .FirstOrDefault(r => r.Name == name);
        if (duplicate is not null)
        {
            throw new InvalidOperationException(
                $"A bus named '{name}' is already registered. Bus names must be unique; " +
                "pick a different name for the additional bus.");
        }

        var builder = new BusBuilder(services, name);
        configure(builder);

        var config = builder.Validate();
        var mode = builder.Mode;

        services.AddSingleton(new BusRegistration { Name = name, Mode = mode, SystemName = config.SystemName });

        services.AddKeyedSingleton<IMessagingTransport>(name, (sp, _) =>
            config.CreateTransport(new TransportContext(sp, name, mode)));

        config.ConfigureServices(services, name);

        services.AddKeyedSingleton<IBus>(name, (sp, _) =>
            new Bus(name, mode, sp, sp.GetRequiredService<IMessageSerializerProvider>()));

        // Each consuming bus gets its own hosted service so buses subscribe, run, and shut
        // down independently. A PublishOnly bus can't have consumers (validated above).
        if (builder.Consumers.Count > 0)
        {
            services.AddSingleton<IHostedService>(sp =>
                ActivatorUtilities.CreateInstance<MessageConsumerHostedService>(sp, name));
        }

        return configurator;
    }
}
