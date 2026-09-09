using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.ServiceBus;

/// <summary>
/// Transport config for Azure Service Bus. Attach to a bus with
/// <c>bus.AddTransport(new AzureServiceBusConfig { ConnectionString = ... })</c> (or the delegate /
/// <c>IConfiguration</c>-binding <c>AddTransport</c> overloads). A topic maps to a Service Bus
/// topic for publishing and to a subscription (via <see cref="TopicSubscriptionMap"/>) for consuming.
/// Registers a health check named <c>runax:{bus}</c> unless
/// <see cref="TransportConfig.RegisterHealthCheck"/> is disabled.
/// </summary>
public sealed class AzureServiceBusConfig : TransportConfig
{
    /// <inheritdoc />
    public override string SystemName => AzureServiceBusTransport.TransportName;

    /// <inheritdoc />
    protected override IMessagingTransport CreateTransport(TransportContext context) =>
        new AzureServiceBusTransport(this, context.Services.GetRequiredService<ILogger<AzureServiceBusTransport>>());

    /// <inheritdoc />
    protected override void ConfigureServices(IServiceCollection services, string busName)
    {
        if (!RegisterHealthCheck)
            return;

        services.AddHealthChecks().Add(new HealthCheckRegistration(
            $"runax:{busName}",
            sp => new AzureServiceBusHealthCheck(sp.GetRequiredKeyedService<IMessagingTransport>(busName)),
            failureStatus: null,
            tags: null));
    }

    /// <summary>
    /// Gets or sets the Service Bus connection string. Required.
    /// </summary>
    [Required]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets a mapping from topic name to the subscription used to consume it.
    /// A topic must have an entry here to be consumable.
    /// </summary>
    // ReSharper disable once CollectionNeverUpdated.Global — populated by consumers via the configure action.
    public Dictionary<string, string> TopicSubscriptionMap { get; set; } = new();

    /// <summary>
    /// Gets or sets the maximum number of messages processed concurrently per subscription. Defaults to 1.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxConcurrentCalls { get; set; } = 1;
}
