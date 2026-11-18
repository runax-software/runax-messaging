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
        var headerSettings = new BusHeaderSettings(builder.WriteNativeHeaders, builder.HeaderPrecedence);
        var contractSettings = new BusContractSettings(builder.RequireContractIdentity);

        services.AddSingleton(new BusRegistration { Name = name, Mode = mode, SystemName = config.SystemName });
        services.AddKeyedSingleton(name, headerSettings);
        services.AddKeyedSingleton(name, contractSettings);

        services.AddKeyedSingleton<IMessagingTransport>(name, (sp, _) =>
            config.CreateTransport(new TransportContext(sp, name, mode)));

        config.ConfigureServices(services, name);

        services.AddKeyedSingleton<IBus>(name, (sp, _) =>
            new Bus(name, mode, headerSettings, sp, sp.GetRequiredService<IMessageSerializerProvider>()));

        // A PublishOnly bus can't have consumers (validated above).
        if (builder.Consumers.Count > 0)
            AddConsumerHost(services, name);

        return configurator;
    }

    /// <summary>
    /// Adds a consumer from a delegate to a bus, outside that bus's <c>AddBus</c> block. Use it when
    /// the code that consumes doesn't own the bus's setup, such as a library adding subscriptions to
    /// a bus its host application registered. The bus may be added before or after this call.
    /// </summary>
    /// <remarks>
    /// The consumer behaves exactly like one registered with
    /// <see cref="BusBuilder.AddConsumer(string, Func{MessageContext, CancellationToken, ValueTask})"/>
    /// inside the block. Whether the bus exists and can consume is checked when the host starts: a
    /// missing bus or a <see cref="BusMode.PublishOnly"/> one fails startup.
    /// </remarks>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="busName">The name of the bus to consume on.</param>
    /// <param name="topic">The topic to consume.</param>
    /// <param name="handler">Handles each message. Throw to retry it per the bus's policy.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    /// <exception cref="ArgumentException"><paramref name="busName"/> or <paramref name="topic"/> is null or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> is null.</exception>
    public static MessagingConfigurator AddConsumer(
        this MessagingConfigurator configurator,
        string busName,
        string topic,
        Func<MessageContext, CancellationToken, ValueTask> handler)
    {
        ArgumentException.ThrowIfNullOrEmpty(busName);

        configurator.Services.AddSingleton(ConsumerRegistration.ForDelegate(busName, topic, handler));
        AddConsumerHost(configurator.Services, busName);
        return configurator;
    }

    // Each consuming bus gets one hosted service, so buses subscribe, run, and shut down
    // independently. It's added by whichever comes first, the bus's own block or a consumer added
    // from outside it, so the bus is checked when the host starts rather than here.
    private static void AddConsumerHost(IServiceCollection services, string busName)
    {
        if (services.Any(d => d.ImplementationInstance is ConsumerHostMarker marker && marker.Bus == busName))
            return;

        services.AddSingleton(new ConsumerHostMarker(busName));
        services.AddSingleton<IHostedService>(sp =>
        {
            var bus = sp.GetServices<BusRegistration>().FirstOrDefault(r => r.Name == busName)
                ?? throw new InvalidOperationException(
                    $"A consumer was added to bus '{busName}', but no bus with that name is registered. " +
                    $"Add it with messaging.AddBus(\"{busName}\", ...).");

            if (bus.Mode == BusMode.PublishOnly)
            {
                var consumer = sp.GetServices<ConsumerRegistration>().First(r => r.Bus == busName);
                throw new InvalidOperationException(
                    $"Bus '{busName}' is PublishOnly, but {consumer.Description} was registered on it.");
            }

            return ActivatorUtilities.CreateInstance<MessageConsumerHostedService>(
                sp,
                busName,
                sp.GetRequiredKeyedService<BusHeaderSettings>(busName),
                sp.GetRequiredKeyedService<BusContractSettings>(busName));
        });
    }

    private sealed record ConsumerHostMarker(string Bus);
}
