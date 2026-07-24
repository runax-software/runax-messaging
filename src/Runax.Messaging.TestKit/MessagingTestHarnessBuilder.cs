using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.TestKit;

/// <summary>
/// Fluent builder for a <see cref="MessagingTestHarness"/>. Register the consumers under test, any services
/// they depend on, and optional messaging tweaks, then call <see cref="StartAsync"/> to spin up a running
/// host over the in-memory transport.
/// </summary>
public sealed class MessagingTestHarnessBuilder
{
    private readonly List<Action<MessagingConfigurator>> _configureMessaging = [];
    private readonly List<Action<IServiceCollection>> _configureServices = [];

    internal MessagingTestHarnessBuilder()
    {
    }

    /// <summary>
    /// Registers a consumer under test. The consumer subscribes to its topic on the harness's in-memory
    /// transport, exactly as <c>AddConsumer&lt;TConsumer&gt;()</c> inside an <c>AddInMemory</c> block would.
    /// </summary>
    /// <typeparam name="TConsumer">The consumer type to register.</typeparam>
    /// <returns>The same builder, to allow chaining.</returns>
    public MessagingTestHarnessBuilder AddConsumer<TConsumer>()
        where TConsumer : class
    {
        // The harness registers a single in-memory transport, so a top-level AddConsumer subscribes exactly
        // there — without re-registering the transport (each AddInMemory call adds another one).
        _configureMessaging.Add(configurator => configurator.AddConsumer<TConsumer>());
        return this;
    }

    /// <summary>
    /// Adds a singleton dependency the consumers under test require (for example a fake repository, a clock, or
    /// an NSubstitute mock). Resolvable by both the consumers and, later, from the running harness.
    /// </summary>
    /// <typeparam name="TService">The service type.</typeparam>
    /// <param name="instance">The instance to register.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public MessagingTestHarnessBuilder AddService<TService>(TService instance)
        where TService : class
    {
        _configureServices.Add(services => services.AddSingleton(instance));
        return this;
    }

    /// <summary>
    /// Adds a singleton dependency the consumers under test require, resolved from the container.
    /// </summary>
    /// <typeparam name="TService">The service type.</typeparam>
    /// <typeparam name="TImplementation">The implementation type.</typeparam>
    /// <returns>The same builder, to allow chaining.</returns>
    public MessagingTestHarnessBuilder AddService<TService, TImplementation>()
        where TService : class
        where TImplementation : class, TService
    {
        _configureServices.Add(services => services.AddSingleton<TService, TImplementation>());
        return this;
    }

    /// <summary>
    /// Escape hatch for registering arbitrary services into the harness's container — use it when the typed
    /// <see cref="AddService{TService}(TService)"/> overloads are not enough.
    /// </summary>
    /// <param name="configure">Action that registers services.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public MessagingTestHarnessBuilder ConfigureServices(Action<IServiceCollection> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _configureServices.Add(configure);
        return this;
    }

    /// <summary>
    /// Escape hatch for configuring messaging directly on the <see cref="MessagingConfigurator"/> — for example
    /// <c>WithRetry(...)</c>, <c>OnUnroutableMessage&lt;T&gt;()</c>, or registering a consumer bound to the
    /// in-memory transport with extra options. The in-memory transport is always registered by the harness.
    /// </summary>
    /// <param name="configure">Action that configures messaging.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public MessagingTestHarnessBuilder ConfigureMessaging(Action<MessagingConfigurator> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _configureMessaging.Add(configure);
        return this;
    }

    /// <summary>
    /// Builds the container, starts dispatch over the in-memory transport, and returns a running
    /// <see cref="MessagingTestHarness"/> ready to publish through and assert against. Dispose the harness
    /// (preferably with <c>await using</c>) to stop the host.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel startup.</param>
    /// <returns>A running harness.</returns>
    public Task<MessagingTestHarness> StartAsync(CancellationToken cancellationToken = default) =>
        MessagingTestHarness.StartAsync(_configureMessaging, _configureServices, cancellationToken);
}
