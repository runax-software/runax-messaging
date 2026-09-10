using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Configures a single bus inside an <c>AddBus</c> block: its one transport, its consumers, and its
/// serialization, retry, and unroutable-message policies. The bus is validated when the block
/// completes — a missing transport, a second transport, or a registration that conflicts with
/// <see cref="Mode"/> throws then, so a misconfigured bus fails at startup.
/// </summary>
public sealed class BusBuilder
{
    private readonly List<Type> _consumers = [];
    private readonly List<string> _consumeSidePolicies = [];
    private readonly List<Action<BusBuilder>> _validations = [];
    private TransportConfig? _transportConfig;

    internal BusBuilder(IServiceCollection services, string busName)
    {
        Services = services;
        BusName = busName;
    }

    /// <summary>Gets the underlying service collection.</summary>
    public IServiceCollection Services { get; }

    /// <summary>Gets the name of the bus being configured.</summary>
    public string BusName { get; }

    /// <summary>
    /// Gets or sets the bus's <see cref="BusMode"/>. Defaults to
    /// <see cref="BusMode.PublishAndConsume"/>; may be set anywhere in the <c>AddBus</c> block
    /// (e.g. <c>bus.Mode = BusMode.ConsumeOnly</c>) — mode rules are validated when the block completes.
    /// </summary>
    public BusMode Mode { get; set; } = BusMode.PublishAndConsume;

    /// <summary>
    /// Gets a shared property bag for extension packages (e.g. the outbox) to keep per-bus
    /// configuration state that participates in end-of-block validation.
    /// </summary>
    public IDictionary<object, object?> Properties { get; } = new Dictionary<object, object?>();

    internal TransportConfig? TransportConfig => _transportConfig;

    internal IReadOnlyList<Type> Consumers => _consumers;

    /// <summary>
    /// Registers a validation callback run when the <c>AddBus</c> block completes. Extension
    /// packages use this to enforce their own configuration rules (e.g. outbox/store pairing).
    /// </summary>
    /// <param name="validation">Callback that throws on invalid configuration.</param>
    public void OnValidate(Action<BusBuilder> validation) => _validations.Add(validation);

    // --- The transport: exactly one per bus -------------------------------------------------

    /// <summary>
    /// Registers this bus's transport from a pre-built config instance.
    /// </summary>
    /// <param name="config">The transport configuration.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    /// <exception cref="InvalidOperationException">The bus already has a transport.</exception>
    public BusBuilder AddTransport(TransportConfig config)
    {
        ArgumentNullException.ThrowIfNull(config);

        if (_transportConfig is not null)
        {
            throw new InvalidOperationException(
                $"Bus '{BusName}' already has a transport ('{_transportConfig.SystemName}'). A bus wraps exactly " +
                "one transport — register an additional bus (messaging.AddBus(\"<name>\", ...)) for an additional broker.");
        }

        _transportConfig = config;
        return this;
    }

    /// <summary>
    /// Creates the config type and applies the delegate:
    /// <c>bus.AddTransport&lt;RabbitMqConfig&gt;(c =&gt; c.Uri = ...)</c>.
    /// </summary>
    /// <typeparam name="TConfig">The transport config type.</typeparam>
    /// <param name="configure">Action that configures the new config instance.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    /// <exception cref="InvalidOperationException">The bus already has a transport.</exception>
    public BusBuilder AddTransport<TConfig>(Action<TConfig> configure)
        where TConfig : TransportConfig, new()
    {
        ArgumentNullException.ThrowIfNull(configure);

        var config = new TConfig();
        configure(config);
        return AddTransport(config);
    }

    /// <summary>
    /// Creates the config type and binds it from a configuration section.
    /// </summary>
    /// <typeparam name="TConfig">The transport config type.</typeparam>
    /// <param name="section">The configuration section to bind the config from.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    /// <exception cref="InvalidOperationException">The bus already has a transport.</exception>
    public BusBuilder AddTransport<TConfig>(IConfiguration section)
        where TConfig : TransportConfig, new()
    {
        ArgumentNullException.ThrowIfNull(section);

        var config = new TConfig();
        section.Bind(config);
        return AddTransport(config);
    }

    // --- Consumers --------------------------------------------------------------------------

    /// <summary>
    /// Registers a message consumer subscribed on this bus's transport. Register the same
    /// consumer type on several buses to consume from each of them.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type to register.</typeparam>
    /// <returns>The same builder, to allow chaining.</returns>
    public BusBuilder AddConsumer<TConsumer>()
        where TConsumer : class
    {
        Services.TryAddSingleton<TConsumer>();
        Services.AddSingleton(new ConsumerRegistration { ConsumerType = typeof(TConsumer), Bus = BusName });
        _consumers.Add(typeof(TConsumer));
        return this;
    }

    // --- Retry / dead-letter policy -----------------------------------------------------------

    /// <summary>
    /// Configures the retry and dead-letter policy applied to this bus's consumers.
    /// </summary>
    /// <param name="configure">Action to configure <see cref="RetryOptions"/>.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public BusBuilder WithRetry(Action<RetryOptions> configure)
    {
        _consumeSidePolicies.Add(nameof(WithRetry));

        Services
            .AddOptions<RetryOptions>(BusName)
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        Services.AddSingleton(new ScopedRetryMarker { Bus = BusName, OptionsName = BusName });
        return this;
    }

    /// <summary>
    /// Configures the retry and dead-letter policy for a single topic on this bus — the most
    /// specific scope, winning over the bus-wide <see cref="WithRetry"/> policy and the built-in
    /// defaults. The policy starts from the <see cref="RetryOptions"/> defaults with
    /// <paramref name="configure"/> applied on top.
    /// </summary>
    /// <param name="topic">The topic whose consumers use this policy.</param>
    /// <param name="configure">Action to configure this topic's <see cref="RetryOptions"/>.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public BusBuilder WithRetryForTopic(string topic, Action<RetryOptions> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        _consumeSidePolicies.Add(nameof(WithRetryForTopic));

        var optionsName = RetryOptionsName.Topic(BusName, topic);
        Services
            .AddOptions<RetryOptions>(optionsName)
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        Services.AddSingleton(new ScopedRetryMarker { Bus = BusName, Topic = topic, OptionsName = optionsName });
        return this;
    }

    // --- Unroutable messages ------------------------------------------------------------------

    /// <summary>
    /// Selects a built-in strategy for messages on this bus that no registered consumer accepts
    /// (an unhandled contract version). Defaults to <see cref="UnroutableStrategy.DeadLetter"/>.
    /// </summary>
    /// <param name="strategy">The strategy to apply.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public BusBuilder OnUnroutableMessage(UnroutableStrategy strategy)
    {
        _consumeSidePolicies.Add(nameof(OnUnroutableMessage));

        Services.AddKeyedSingleton<IUnroutableMessageHandler>(BusName, (_, _) => strategy switch
        {
            UnroutableStrategy.Requeue => new RequeueUnroutableHandler(),
            UnroutableStrategy.Discard => new DiscardUnroutableHandler(),
            _ => new DeadLetterUnroutableHandler(),
        });

        return this;
    }

    /// <summary>
    /// Registers a custom <see cref="IUnroutableMessageHandler"/> for messages on this bus that no
    /// registered consumer accepts — for example to forward them to a quarantine topic.
    /// </summary>
    /// <typeparam name="THandler">The handler implementation.</typeparam>
    /// <returns>The same builder, to allow chaining.</returns>
    public BusBuilder OnUnroutableMessage<THandler>()
        where THandler : class, IUnroutableMessageHandler
    {
        _consumeSidePolicies.Add(nameof(OnUnroutableMessage));
        Services.AddKeyedSingleton<IUnroutableMessageHandler, THandler>(BusName);
        return this;
    }

    // --- Serialization ------------------------------------------------------------------------

    /// <summary>
    /// Configures the <see cref="JsonSerializerOptions"/> used for this bus. The options start as
    /// a copy of the container's global <see cref="JsonSerializerOptions"/> with
    /// <paramref name="configure"/> applied on top, so the bus inherits application-wide settings
    /// and overrides just what it needs.
    /// </summary>
    /// <param name="configure">Action to configure this bus's <see cref="JsonSerializerOptions"/>.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public BusBuilder ConfigureSerialization(Action<JsonSerializerOptions> configure)
    {
        Services.AddKeyedSingleton<ISerializer>(BusName, (sp, _) =>
        {
            var options = new JsonSerializerOptions(sp.GetRequiredService<JsonSerializerOptions>());
            configure(options);
            return new SystemTextJsonSerializer(options);
        });
        return this;
    }

    /// <summary>
    /// Replaces the body serializer for this bus — for example a source-generated or third-party
    /// JSON serializer. This changes only how message bodies are encoded; the framework's reserved
    /// <c>__runax</c> envelope is always applied around the body and stays identical.
    /// </summary>
    /// <typeparam name="TSerializer">The body serializer implementation.</typeparam>
    /// <returns>The same builder, to allow chaining.</returns>
    public BusBuilder UseSerializer<TSerializer>()
        where TSerializer : class, ISerializer
    {
        Services.AddKeyedSingleton<ISerializer, TSerializer>(BusName);
        return this;
    }

    /// <summary>
    /// Configures the <see cref="JsonSerializerOptions"/> used for a single topic on this bus —
    /// the most specific selection, winning over the bus serializer. The options start as a copy
    /// of the container's global options with <paramref name="configure"/> applied on top.
    /// </summary>
    /// <param name="topic">The topic whose messages use these options.</param>
    /// <param name="configure">Action to configure this topic's <see cref="JsonSerializerOptions"/>.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public BusBuilder ConfigureSerializationForTopic(string topic, Action<JsonSerializerOptions> configure)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        Services.AddKeyedSingleton<ISerializer>(new TopicKey(BusName, topic), (sp, _) =>
        {
            var options = new JsonSerializerOptions(sp.GetRequiredService<JsonSerializerOptions>());
            configure(options);
            return new SystemTextJsonSerializer(options);
        });
        return this;
    }

    /// <summary>
    /// Replaces the body serializer for a single topic on this bus — the most specific selection,
    /// winning over the bus serializer. As with <see cref="UseSerializer{TSerializer}"/>, only the
    /// body encoding changes; the reserved <c>__runax</c> envelope is unaffected.
    /// </summary>
    /// <typeparam name="TSerializer">The body serializer implementation.</typeparam>
    /// <param name="topic">The topic whose messages use this serializer.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public BusBuilder UseSerializerForTopic<TSerializer>(string topic)
        where TSerializer : class, ISerializer
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        Services.AddKeyedSingleton<ISerializer, TSerializer>(new TopicKey(BusName, topic));
        return this;
    }

    // --- End-of-block validation ----------------------------------------------------------------

    internal TransportConfig Validate()
    {
        if (_transportConfig is null)
        {
            throw new InvalidOperationException(
                $"Bus '{BusName}' has no transport. Register exactly one with bus.AddTransport(...) " +
                "(e.g. bus.AddTransport(new InMemoryConfig())).");
        }

        if (Mode == BusMode.PublishOnly)
        {
            if (_consumers.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Bus '{BusName}' is PublishOnly, but consumer '{_consumers[0].Name}' was registered on it.");
            }

            if (_consumeSidePolicies.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Bus '{BusName}' is PublishOnly, but the consume-side policy '{_consumeSidePolicies[0]}' " +
                    "was configured on it.");
            }
        }

        var validationResults = new List<ValidationResult>();
        if (!Validator.TryValidateObject(
                _transportConfig, new ValidationContext(_transportConfig), validationResults, validateAllProperties: true))
        {
            var errors = string.Join("; ", validationResults.Select(r => r.ErrorMessage));
            throw new InvalidOperationException(
                $"Bus '{BusName}': the {_transportConfig.GetType().Name} transport config is invalid: {errors}");
        }

        foreach (var validation in _validations)
            validation(this);

        return _transportConfig;
    }
}
