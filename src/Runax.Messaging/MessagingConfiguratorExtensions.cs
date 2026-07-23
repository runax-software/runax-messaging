using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Configurator extensions for registering consumers.
/// </summary>
public static class MessagingConfiguratorExtensions
{
    /// <summary>
    /// Registers a message consumer to be started by the hosted service.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type to register.</typeparam>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="transports">
    /// The system names of the transports to subscribe this consumer on (matched against
    /// <see cref="IMessagingTransport.SystemName"/>, e.g. <c>"rabbitmq"</c>, <c>"sqs"</c>). When none are
    /// given the consumer subscribes on every registered transport, letting a single consumer receive
    /// its topic from several — possibly different — brokers at once.
    /// </param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddConsumer<TConsumer>(
        this MessagingConfigurator configurator,
        params string[] transports)
        where TConsumer : class
    {
        configurator.Services.AddSingleton<TConsumer>();
        configurator.Services.AddSingleton(new ConsumerRegistration
        {
            ConsumerType = typeof(TConsumer),
            Transports = transports.Length > 0 ? transports : null,
        });
        return configurator;
    }

    /// <summary>
    /// Selects which registered transport <see cref="IMessagePublisher"/> publishes to when more than one
    /// transport is configured. Not needed when a single transport is registered.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="transport">The target transport's <see cref="IMessagingTransport.SystemName"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator PublishTo(this MessagingConfigurator configurator, string transport)
    {
        configurator.Services.AddSingleton(new MessagingPublishOptions { DefaultTransport = transport });
        return configurator;
    }

    /// <summary>
    /// Selects a built-in strategy for messages that no registered consumer accepts (an unhandled contract
    /// version). Defaults to <see cref="UnroutableStrategy.DeadLetter"/> when not configured.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="strategy">The strategy to apply.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator OnUnroutableMessage(
        this MessagingConfigurator configurator,
        UnroutableStrategy strategy)
    {
        configurator.Services.AddSingleton<IUnroutableMessageHandler>(_ => strategy switch
        {
            UnroutableStrategy.Requeue => new RequeueUnroutableHandler(),
            UnroutableStrategy.Discard => new DiscardUnroutableHandler(),
            _ => new DeadLetterUnroutableHandler(),
        });

        return configurator;
    }

    /// <summary>
    /// Registers a custom <see cref="IUnroutableMessageHandler"/> for messages that no registered consumer
    /// accepts — for example to forward them to a quarantine topic or raise an alert.
    /// </summary>
    /// <typeparam name="THandler">The handler implementation.</typeparam>
    /// <param name="configurator">The messaging configurator.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator OnUnroutableMessage<THandler>(this MessagingConfigurator configurator)
        where THandler : class, IUnroutableMessageHandler
    {
        configurator.Services.AddSingleton<IUnroutableMessageHandler, THandler>();
        return configurator;
    }

    /// <summary>
    /// Configures the retry and dead-letter policy applied to all consumers.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="RetryOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator WithRetry(
        this MessagingConfigurator configurator,
        Action<RetryOptions> configure)
    {
        configurator.Services
            .AddOptions<RetryOptions>()
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return configurator;
    }

    /// <summary>
    /// Configures the <see cref="JsonSerializerOptions"/> used to serialize and deserialize message bodies.
    /// Set a <see cref="System.Text.Json.Serialization.Metadata.IJsonTypeInfoResolver"/> here (e.g. a
    /// source-generated <c>JsonSerializerContext</c>) for a trim-friendly / AOT path.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure the shared <see cref="JsonSerializerOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator ConfigureSerialization(
        this MessagingConfigurator configurator,
        Action<JsonSerializerOptions> configure)
    {
        configurator.Services.Configure(configure);
        return configurator;
    }

    /// <summary>
    /// Replaces the default serializer with a custom <see cref="IMessageSerializer"/> — for example to read a
    /// non-Runax wire format such as CloudEvents. The default already reads foreign/raw JSON (any payload without
    /// the reserved <c>__runax</c> key) as-is, so a custom serializer is only needed for other encodings.
    /// </summary>
    /// <typeparam name="TSerializer">The serializer implementation.</typeparam>
    /// <param name="configurator">The messaging configurator.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator UseSerializer<TSerializer>(this MessagingConfigurator configurator)
        where TSerializer : class, IMessageSerializer
    {
        configurator.Services.AddSingleton<IMessageSerializer, TSerializer>();
        return configurator;
    }
}
