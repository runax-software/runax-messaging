using System.ComponentModel.DataAnnotations;

namespace Runax.Messaging.Transports.Google.PubSub;

/// <summary>
/// Configuration options for the Google Cloud Pub/Sub messaging transport.
/// </summary>
public sealed class GooglePubSubOptions
{
    /// <summary>
    /// Gets or sets the Google Cloud project id. Required.
    /// </summary>
    [Required]
    public string ProjectId { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets a mapping from topic name to the subscription id used to consume it.
    /// Topics without an entry consume from a subscription named after the topic.
    /// </summary>
    // ReSharper disable once CollectionNeverUpdated.Global — populated by consumers via the configure action.
    public Dictionary<string, string> TopicSubscriptionMap { get; set; } = new();
}
