using System.ComponentModel.DataAnnotations;
using global::Azure.Messaging.EventHubs.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.EventHubs;

/// <summary>
/// Transport config for Azure Event Hubs. Attach to a bus with
/// <c>bus.AddTransport(new AzureEventHubsConfig { ConnectionString = ... })</c> (or the delegate /
/// <c>IConfiguration</c>-binding <c>AddTransport</c> overloads). A runax topic maps to an event hub
/// of the same name. Consuming uses an <c>EventProcessorClient</c> over a consumer group with a
/// blob checkpoint store, so <see cref="BlobConnectionString"/> and <see cref="BlobContainerName"/>
/// are required to subscribe. Registers a health check named <c>runax:{bus}</c> unless
/// <see cref="TransportConfig.RegisterHealthCheck"/> is disabled.
/// </summary>
public sealed class AzureEventHubsConfig : TransportConfig
{
    /// <inheritdoc />
    public override string SystemName => AzureEventHubsTransport.TransportName;

    /// <inheritdoc />
    protected override IMessagingTransport CreateTransport(TransportContext context) =>
        new AzureEventHubsTransport(this, context.Services.GetRequiredService<ILogger<AzureEventHubsTransport>>());

    /// <inheritdoc />
    protected override void ConfigureServices(IServiceCollection services, string busName)
    {
        if (!RegisterHealthCheck)
            return;

        services.AddHealthChecks().Add(new HealthCheckRegistration(
            $"runax:{busName}",
            sp => new AzureEventHubsHealthCheck(sp.GetRequiredKeyedService<IMessagingTransport>(busName)),
            failureStatus: null,
            tags: null));
    }

    /// <summary>
    /// Gets or sets the fully qualified Event Hubs namespace (e.g. <c>my-ns.servicebus.windows.net</c>),
    /// used together with a <c>DefaultAzureCredential</c> (Azure Identity). Either this or
    /// <see cref="ConnectionString"/> must be set.
    /// </summary>
    public string? FullyQualifiedNamespace { get; set; }

    /// <summary>
    /// Gets or sets the Event Hubs namespace connection string. Either this or
    /// <see cref="FullyQualifiedNamespace"/> must be set. When both are set, the connection string wins.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Gets or sets the consumer group used when subscribing. Defaults to the default consumer group ("$Default").
    /// </summary>
    [Required]
    public string ConsumerGroup { get; set; } = EventHubConsumerClient.DefaultConsumerGroupName;

    /// <summary>
    /// Gets or sets the Azure Storage connection string for the blob checkpoint store used while consuming.
    /// Required to subscribe.
    /// </summary>
    public string? BlobConnectionString { get; set; }

    /// <summary>
    /// Gets or sets the blob container that holds ownership and checkpoint state for the processor.
    /// Required to subscribe.
    /// </summary>
    public string? BlobContainerName { get; set; }

    /// <summary>
    /// Gets or sets whether a <c>DeadLetter</c> disposition republishes the envelope to a companion
    /// <c>{topic}.dead-letter</c> event hub. When <c>false</c> (the default), dead-lettered messages are
    /// logged and checkpointed (dropped), since Event Hubs has no native dead-letter facility. When
    /// <c>true</c>, the <c>{topic}.dead-letter</c> hub must be provisioned ahead of time.
    /// </summary>
    public bool ProduceDeadLetterHub { get; set; }

    /// <summary>
    /// Gets the suffix appended to a topic to form its dead-letter event hub name.
    /// </summary>
    internal const string DeadLetterHubSuffix = ".dead-letter";

    /// <summary>
    /// Validates that connection information is present and, when subscribing is possible, that the
    /// checkpoint store is configured.
    /// </summary>
    internal void EnsureConnectionConfigured()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString) && string.IsNullOrWhiteSpace(FullyQualifiedNamespace))
        {
            throw new ValidationException(
                "Either ConnectionString or FullyQualifiedNamespace must be set for the Azure Event Hubs transport.");
        }
    }

    /// <summary>
    /// Validates that the blob checkpoint store is configured for consuming.
    /// </summary>
    internal void EnsureCheckpointStoreConfigured()
    {
        if (string.IsNullOrWhiteSpace(BlobConnectionString) || string.IsNullOrWhiteSpace(BlobContainerName))
        {
            throw new ValidationException(
                "BlobConnectionString and BlobContainerName must be set to consume from Azure Event Hubs.");
        }
    }
}
