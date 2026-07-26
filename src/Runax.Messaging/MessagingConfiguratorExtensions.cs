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
    /// Selects a built-in strategy for messages that no registered consumer accepts on this transport only —
    /// messages on other brokers keep the global strategy (or the built-in <see cref="UnroutableStrategy.DeadLetter"/>
    /// default). Call inside a transport's configuration block
    /// (e.g. <c>AddRabbitMq(rabbit =&gt; rabbit.OnUnroutableMessage(UnroutableStrategy.Discard))</c>).
    /// </summary>
    /// <param name="builder">The transport builder for the broker to scope to.</param>
    /// <param name="strategy">The strategy to apply.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static TransportBuilder OnUnroutableMessage(
        this TransportBuilder builder,
        UnroutableStrategy strategy)
    {
        builder.Services.AddKeyedSingleton<IUnroutableMessageHandler>(builder.TransportName, (_, _) => strategy switch
        {
            UnroutableStrategy.Requeue => new RequeueUnroutableHandler(),
            UnroutableStrategy.Discard => new DiscardUnroutableHandler(),
            _ => new DeadLetterUnroutableHandler(),
        });

        return builder;
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
    /// Registers a custom <see cref="IUnroutableMessageHandler"/> for unroutable messages on this transport only —
    /// messages on other brokers keep the global handler (or the built-in default). Call inside a transport's
    /// configuration block (e.g. <c>AddRabbitMq(rabbit =&gt; rabbit.OnUnroutableMessage&lt;MyHandler&gt;())</c>).
    /// </summary>
    /// <typeparam name="THandler">The handler implementation.</typeparam>
    /// <param name="builder">The transport builder for the broker to scope to.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static TransportBuilder OnUnroutableMessage<THandler>(this TransportBuilder builder)
        where THandler : class, IUnroutableMessageHandler
    {
        builder.Services.AddKeyedSingleton<IUnroutableMessageHandler, THandler>(builder.TransportName);
        return builder;
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
    /// Configures the retry and dead-letter policy for consumers on this transport only — consumers on other
    /// brokers keep the global policy (or the built-in defaults when no global <c>WithRetry</c> was called). Call
    /// inside a transport's configuration block (e.g. <c>AddRabbitMq(rabbit =&gt; rabbit.WithRetry(o =&gt; o.MaxAttempts = 5))</c>).
    /// The scoped policy starts from the <see cref="RetryOptions"/> defaults with <paramref name="configure"/>
    /// applied on top and is validated with the same DataAnnotations as the global policy.
    /// </summary>
    /// <param name="builder">The transport builder for the broker to scope to.</param>
    /// <param name="configure">Action to configure this broker's <see cref="RetryOptions"/>.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static TransportBuilder WithRetry(
        this TransportBuilder builder,
        Action<RetryOptions> configure)
    {
        builder.Services
            .AddOptions<RetryOptions>(builder.TransportName)
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        builder.Services.AddSingleton(new ScopedRetryMarker
        {
            Transport = builder.TransportName,
            OptionsName = builder.TransportName,
        });

        return builder;
    }

    /// <summary>
    /// Configures the retry and dead-letter policy for a single topic on every transport — messages on other
    /// topics keep the per-broker policy (when one is registered), else the global policy, else the built-in
    /// defaults. A per-topic policy is the most specific scope alongside
    /// <see cref="WithRetryForTopic(TransportBuilder, string, Action{RetryOptions})"/>, winning over a per-broker
    /// policy for the same topic. The policy starts from the <see cref="RetryOptions"/> defaults with
    /// <paramref name="configure"/> applied on top and is validated with the same DataAnnotations as the global
    /// policy.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="topic">The topic whose consumers use this policy.</param>
    /// <param name="configure">Action to configure this topic's <see cref="RetryOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator WithRetryForTopic(
        this MessagingConfigurator configurator,
        string topic,
        Action<RetryOptions> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);

        var optionsName = RetryOptionsName.Topic(topic);
        configurator.Services
            .AddOptions<RetryOptions>(optionsName)
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        configurator.Services.AddSingleton(new ScopedRetryMarker { Topic = topic, OptionsName = optionsName });

        return configurator;
    }

    /// <summary>
    /// Configures the retry and dead-letter policy for a single topic on this transport only — the narrowest
    /// scope, winning over a global per-topic policy, this broker's per-broker policy, the global policy, and the
    /// built-in defaults. Call inside a transport's configuration block
    /// (e.g. <c>AddKafka(kafka =&gt; kafka.WithRetryForTopic("payments", o =&gt; o.MaxAttempts = 10))</c>). The
    /// policy starts from the <see cref="RetryOptions"/> defaults with <paramref name="configure"/> applied on top.
    /// </summary>
    /// <param name="builder">The transport builder for the broker to scope to.</param>
    /// <param name="topic">The topic whose consumers use this policy on this transport.</param>
    /// <param name="configure">Action to configure this topic's <see cref="RetryOptions"/>.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static TransportBuilder WithRetryForTopic(
        this TransportBuilder builder,
        string topic,
        Action<RetryOptions> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);

        var optionsName = RetryOptionsName.TransportTopic(builder.TransportName, topic);
        builder.Services
            .AddOptions<RetryOptions>(optionsName)
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        builder.Services.AddSingleton(new ScopedRetryMarker
        {
            Transport = builder.TransportName,
            Topic = topic,
            OptionsName = optionsName,
        });

        return builder;
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

    /// <summary>
    /// Replaces the body serializer for a single topic on every transport — messages on other topics keep the
    /// per-broker serializer (when one is registered) or the global serializer. This is the most specific
    /// selection alongside <see cref="UseSerializerForTopic{TSerializer}(TransportBuilder, string)"/>: a topic
    /// serializer wins over a per-broker one for the same topic. As with the other overloads, this changes only
    /// the body; the reserved <c>__runax</c> envelope is unaffected.
    /// </summary>
    /// <typeparam name="TSerializer">The body serializer implementation.</typeparam>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="topic">The topic whose messages use this serializer.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator UseSerializerForTopic<TSerializer>(
        this MessagingConfigurator configurator,
        string topic)
        where TSerializer : class, ISerializer
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        configurator.Services.AddKeyedSingleton<ISerializer, TSerializer>(new TopicSerializerKey(topic));
        return configurator;
    }

    /// <summary>
    /// Configures the <see cref="JsonSerializerOptions"/> used for a single topic on every transport. The options
    /// start as a copy of the global options (from <see cref="ConfigureSerialization(MessagingConfigurator, Action{JsonSerializerOptions})"/>)
    /// with <paramref name="configure"/> applied on top, so the topic inherits global settings and overrides just
    /// what it needs.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="topic">The topic whose messages use these options.</param>
    /// <param name="configure">Action to configure this topic's <see cref="JsonSerializerOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator ConfigureSerializationForTopic(
        this MessagingConfigurator configurator,
        string topic,
        Action<JsonSerializerOptions> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        configurator.Services.AddKeyedSingleton<ISerializer>(new TopicSerializerKey(topic), (sp, _) =>
        {
            var options = new JsonSerializerOptions(sp.GetRequiredService<JsonSerializerOptions>());
            configure(options);
            return new SystemTextJsonSerializer(options);
        });
        return configurator;
    }

    /// <summary>
    /// Replaces the body serializer for a single topic on this transport only — the narrowest selection, winning
    /// over a global per-topic serializer, this broker's per-broker serializer, and the global serializer. Call
    /// inside a transport's configuration block
    /// (e.g. <c>AddKafka(kafka =&gt; kafka.UseSerializerForTopic&lt;AvroSerializer&gt;("orders"))</c>). This
    /// changes only the body; the reserved <c>__runax</c> envelope is unaffected.
    /// </summary>
    /// <typeparam name="TSerializer">The body serializer implementation.</typeparam>
    /// <param name="builder">The transport builder for the broker to scope to.</param>
    /// <param name="topic">The topic whose messages use this serializer on this transport.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static TransportBuilder UseSerializerForTopic<TSerializer>(
        this TransportBuilder builder,
        string topic)
        where TSerializer : class, ISerializer
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        builder.Services.AddKeyedSingleton<ISerializer, TSerializer>(
            new TransportTopicSerializerKey(builder.TransportName, topic));
        return builder;
    }

    /// <summary>
    /// Configures the <see cref="JsonSerializerOptions"/> used for a single topic on this transport only. The
    /// options start as a copy of the global options with <paramref name="configure"/> applied on top. Call
    /// inside a transport's configuration block.
    /// </summary>
    /// <param name="builder">The transport builder for the broker to scope to.</param>
    /// <param name="topic">The topic whose messages use these options on this transport.</param>
    /// <param name="configure">Action to configure this topic's <see cref="JsonSerializerOptions"/>.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static TransportBuilder ConfigureSerializationForTopic(
        this TransportBuilder builder,
        string topic,
        Action<JsonSerializerOptions> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        builder.Services.AddKeyedSingleton<ISerializer>(
            new TransportTopicSerializerKey(builder.TransportName, topic), (sp, _) =>
            {
                var options = new JsonSerializerOptions(sp.GetRequiredService<JsonSerializerOptions>());
                configure(options);
                return new SystemTextJsonSerializer(options);
            });
        return builder;
    }
}
