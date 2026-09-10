using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Google.PubSub;

/// <summary>
/// Transport config for Google Cloud Pub/Sub. Attach to a bus with
/// <c>bus.AddTransport(new GooglePubSubConfig { ProjectId = ... })</c> (or the delegate /
/// <c>IConfiguration</c>-binding <c>AddTransport</c> overloads). Registers a health check named
/// <c>runax:{bus}</c> unless <see cref="TransportConfig.RegisterHealthCheck"/> is disabled.
/// </summary>
public sealed class GooglePubSubConfig : TransportConfig
{
    /// <inheritdoc />
    public override string SystemName => GooglePubSubTransport.TransportName;

    /// <inheritdoc />
    protected override IMessagingTransport CreateTransport(TransportContext context) =>
        new GooglePubSubTransport(this, context.Services.GetRequiredService<ILogger<GooglePubSubTransport>>());

    /// <inheritdoc />
    protected override void ConfigureServices(IServiceCollection services, string busName)
    {
        if (!RegisterHealthCheck)
            return;

        services.AddHealthChecks().Add(new HealthCheckRegistration(
            $"runax:{busName}",
            sp => new GooglePubSubHealthCheck(sp.GetRequiredKeyedService<IMessagingTransport>(busName)),
            failureStatus: null,
            tags: null));
    }

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
