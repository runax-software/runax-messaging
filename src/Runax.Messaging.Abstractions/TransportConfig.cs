using Microsoft.Extensions.DependencyInjection;

namespace Runax.Messaging.Abstractions;

/// <summary>
/// Base class for transport configuration — the SPI a transport package implements instead of
/// shipping registration extension methods. A package derives one config type (e.g.
/// <c>RabbitMqConfig</c>), puts its broker settings on it (validated with DataAnnotations at
/// configuration time), and implements <see cref="CreateTransport"/>. Applications attach the
/// config to a bus with <c>bus.AddTransport(...)</c>; a bus wraps exactly one transport.
/// </summary>
public abstract class TransportConfig
{
    /// <summary>
    /// Gets the broker technology identifier (e.g. <c>"rabbitmq"</c>, <c>"sqs"</c>), used as the
    /// OpenTelemetry <c>messaging.system</c> tag. Identity and uniqueness live on the bus name,
    /// not here — any number of buses may use the same system.
    /// </summary>
    public abstract string SystemName { get; }

    /// <summary>
    /// Gets or sets whether the transport auto-registers a health check named
    /// <c>runax:{bus}</c>. Defaults to <see langword="true"/>; ignored by transports that have
    /// no health check (e.g. in-memory).
    /// </summary>
    public bool RegisterHealthCheck { get; set; } = true;

    /// <summary>
    /// Creates the transport for this config. Called once per bus at first use, after the
    /// config has passed DataAnnotations validation.
    /// </summary>
    /// <param name="context">The bus the transport is being created for.</param>
    /// <returns>The transport instance.</returns>
    protected internal abstract IMessagingTransport CreateTransport(TransportContext context);

    /// <summary>
    /// Performs additional service registrations for this transport (health checks, broker SDK
    /// clients). Called once from <c>AddTransport</c>. The default implementation does nothing.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="busName">The name of the bus the transport belongs to.</param>
    protected internal virtual void ConfigureServices(IServiceCollection services, string busName)
    {
    }
}
