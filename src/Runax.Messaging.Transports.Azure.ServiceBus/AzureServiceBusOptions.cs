using System.ComponentModel.DataAnnotations;

namespace Runax.Messaging.Transports.Azure.ServiceBus;

/// <summary>
/// Configuration options for the Azure Service Bus messaging transport. A topic maps to a Service Bus
/// topic for publishing and to a subscription (via <see cref="TopicSubscriptionMap"/>) for consuming.
/// </summary>
public sealed class AzureServiceBusOptions
{
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
