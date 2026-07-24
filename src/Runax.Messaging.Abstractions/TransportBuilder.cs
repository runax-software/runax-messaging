using Microsoft.Extensions.DependencyInjection;

namespace Runax.Messaging.Abstractions;

/// <summary>
/// Scopes registrations (such as consumers) to a single transport. Passed to the configuration block of a
/// transport's <c>Add*</c> method so that anything registered through it binds to that broker.
/// </summary>
public class TransportBuilder(IServiceCollection services, string transportName)
{
    /// <summary>
    /// Gets the underlying service collection.
    /// </summary>
    public IServiceCollection Services { get; } = services;

    /// <summary>
    /// Gets the <see cref="IMessagingTransport.SystemName"/> of the transport this builder scopes to.
    /// </summary>
    public string TransportName { get; } = transportName;
}

/// <summary>
/// A <see cref="TransportBuilder"/> that also configures the transport's options, so options and consumers are
/// set up in one block:
/// <c>AddRabbitMq(rabbit =&gt; { rabbit.Configure(o =&gt; ...); rabbit.AddConsumer&lt;T&gt;(); })</c>.
/// </summary>
/// <typeparam name="TOptions">The transport's options type.</typeparam>
public sealed class TransportBuilder<TOptions>(IServiceCollection services, string transportName)
    : TransportBuilder(services, transportName)
    where TOptions : class
{
    /// <summary>
    /// Gets the accumulated options configuration, applied by the transport's registration extension. Not
    /// normally called directly.
    /// </summary>
    public Action<TOptions>? Configuration { get; private set; }

    /// <summary>
    /// Configures the transport's options. May be called more than once; the actions compose.
    /// </summary>
    /// <param name="configure">Action to configure the options.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public TransportBuilder<TOptions> Configure(Action<TOptions> configure)
    {
        Configuration = Configuration is null ? configure : Configuration + configure;
        return this;
    }
}
