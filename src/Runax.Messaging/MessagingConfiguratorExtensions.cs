using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
        configurator.Services.TryAddSingleton<TConsumer>();
        configurator.Services.AddSingleton(new ConsumerRegistration
        {
            ConsumerType = typeof(TConsumer),
            Transports = transports.Length > 0 ? transports : null,
        });
        return configurator;
    }

    /// <summary>
    /// Registers a consumer scoped to a single transport. Call this inside a transport's configuration block
    /// (e.g. <c>AddRabbitMq(o =&gt; ..., rabbit =&gt; rabbit.AddConsumer&lt;T&gt;())</c>) so the consumer
    /// subscribes only on that broker. Register the same consumer under two transports to consume from both.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type to register.</typeparam>
    /// <param name="builder">The transport builder for the broker to subscribe on.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static TransportBuilder AddConsumer<TConsumer>(this TransportBuilder builder)
        where TConsumer : class
    {
        builder.Services.TryAddSingleton<TConsumer>();
        builder.Services.AddSingleton(new ConsumerRegistration
        {
            ConsumerType = typeof(TConsumer),
            Transports = [builder.TransportName],
        });
        return builder;
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
    /// Replaces the default body serializer with a custom <see cref="ISerializer"/> — for example a
    /// source-generated, case-insensitive, or third-party JSON serializer. This changes only how message
    /// bodies are encoded; the framework's reserved <c>__runax</c> envelope is always applied around the body
    /// and stays identical regardless of which serializer is registered. For simple tweaks (naming policy,
    /// converters) prefer <see cref="ConfigureSerialization(MessagingConfigurator, Action{JsonSerializerOptions})"/>.
    /// </summary>
    /// <typeparam name="TSerializer">The body serializer implementation.</typeparam>
    /// <param name="configurator">The messaging configurator.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator UseSerializer<TSerializer>(this MessagingConfigurator configurator)
        where TSerializer : class, ISerializer
    {
        configurator.Services.AddSingleton<ISerializer, TSerializer>();
        return configurator;
    }

    /// <summary>
    /// Replaces the body serializer for this transport only — messages published to or consumed from other
    /// brokers keep the global serializer. Call inside a transport's configuration block
    /// (e.g. <c>AddRabbitMq(rabbit =&gt; rabbit.UseSerializer&lt;MySerializer&gt;())</c>). As with the global
    /// <see cref="UseSerializer{TSerializer}(MessagingConfigurator)"/>, this changes only the body; the reserved
    /// <c>__runax</c> envelope is unaffected.
    /// </summary>
    /// <typeparam name="TSerializer">The body serializer implementation.</typeparam>
    /// <param name="builder">The transport builder for the broker to scope to.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static TransportBuilder UseSerializer<TSerializer>(this TransportBuilder builder)
        where TSerializer : class, ISerializer
    {
        builder.Services.AddKeyedSingleton<ISerializer, TSerializer>(builder.TransportName);
        return builder;
    }

    /// <summary>
    /// Configures the <see cref="JsonSerializerOptions"/> used for this transport only. The options start as a
    /// copy of the global options (from <see cref="ConfigureSerialization(MessagingConfigurator, Action{JsonSerializerOptions})"/>)
    /// with <paramref name="configure"/> applied on top, so a broker inherits global settings and overrides just
    /// what it needs. Call inside a transport's configuration block.
    /// </summary>
    /// <param name="builder">The transport builder for the broker to scope to.</param>
    /// <param name="configure">Action to configure this broker's <see cref="JsonSerializerOptions"/>.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static TransportBuilder ConfigureSerialization(
        this TransportBuilder builder,
        Action<JsonSerializerOptions> configure)
    {
        builder.Services.AddKeyedSingleton<ISerializer>(builder.TransportName, (sp, _) =>
        {
            var options = new JsonSerializerOptions(sp.GetRequiredService<JsonSerializerOptions>());
            configure(options);
            return new SystemTextJsonSerializer(options);
        });
        return builder;
    }
}
