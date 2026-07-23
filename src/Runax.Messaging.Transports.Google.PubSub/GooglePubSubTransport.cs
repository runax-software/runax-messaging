using System.Collections.Concurrent;
using Google.Api.Gax;
using Google.Api.Gax.ResourceNames;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Google.PubSub;

/// <summary>
/// Google Cloud Pub/Sub implementation of <see cref="IMessagingTransport"/>. A runax topic maps to a
/// Pub/Sub topic for publishing and to a subscription (via <see cref="GooglePubSubOptions.TopicSubscriptionMap"/>)
/// for consuming.
/// </summary>
internal sealed class GooglePubSubTransport(GooglePubSubOptions options, ILogger<GooglePubSubTransport> logger)
    : IMessagingTransport, IDisposable
{
    private readonly ConcurrentDictionary<string, Task<PublisherClient>> _publishers = new();

    internal const string TransportName = "google_pubsub";

    public string SystemName => TransportName;

    public async ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
    {
        var publisher = await GetPublisherAsync(topic).ConfigureAwait(false);
        await publisher.PublishAsync(new PubsubMessage { Data = ByteString.CopyFromUtf8(envelopeJson) }).ConfigureAwait(false);
    }

    private Task<PublisherClient> GetPublisherAsync(string topic) =>
        _publishers.GetOrAdd(topic, t => new PublisherClientBuilder
        {
            TopicName = TopicName.FromProjectTopic(options.ProjectId, t),
            EmulatorDetection = EmulatorDetection.EmulatorOrProduction
        }.BuildAsync());

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        var subscribers = new List<SubscriberClient>();
        var runners = new List<Task>();

        foreach (var topic in topics)
        {
            var subscriptionId = options.TopicSubscriptionMap.GetValueOrDefault(topic, topic);
            var subscriptionName = SubscriptionName.FromProjectSubscription(options.ProjectId, subscriptionId);

            var subscriber = await new SubscriberClientBuilder
            {
                SubscriptionName = subscriptionName,
                EmulatorDetection = EmulatorDetection.EmulatorOrProduction
            }.BuildAsync(cancellationToken).ConfigureAwait(false);

            subscribers.Add(subscriber);
            var subscriptionTopic = topic;
            runners.Add(subscriber.StartAsync(async (message, _) =>
            {
                MessageDisposition disposition;
                try
                {
                    disposition = await onMessage(message.Data.ToStringUtf8(), subscriptionTopic).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Unexpected error dispatching Pub/Sub message on '{Topic}'; nacking.", subscriptionTopic);
                    disposition = MessageDisposition.Requeue;
                }

                // Pub/Sub has no explicit reject: Nack redelivers, and a subscription dead-letter policy
                // moves the message to its dead-letter topic once maxDeliveryAttempts is reached.
                return disposition == MessageDisposition.Acknowledge
                    ? SubscriberClient.Reply.Ack
                    : SubscriberClient.Reply.Nack;
            }));

            logger.LogInformation("Subscribed to Pub/Sub subscription {Subscription} for topic {Topic}", subscriptionName, topic);
        }

        await using var registration = cancellationToken.Register(() =>
        {
            var shutdown = new SubscriberClient.ShutdownOptions
            {
                Mode = SubscriberClient.ShutdownMode.WaitForProcessing,
                Timeout = TimeSpan.FromSeconds(15)
            };
            foreach (var subscriber in subscribers)
                _ = subscriber.StopAsync(shutdown, CancellationToken.None);
        });

        try
        {
            await Task.WhenAll(runners).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Graceful shutdown.
        }
    }

    /// <summary>
    /// Verifies reachability by listing the project's topics through the admin API.
    /// </summary>
    internal async Task<bool> PingAsync(CancellationToken cancellationToken = default)
    {
        var api = await new PublisherServiceApiClientBuilder
        {
            EmulatorDetection = EmulatorDetection.EmulatorOrProduction
        }.BuildAsync(cancellationToken).ConfigureAwait(false);

        var projectName = ProjectName.FromProject(options.ProjectId);
        await foreach (var _ in api.ListTopicsAsync(projectName).WithCancellation(cancellationToken).ConfigureAwait(false))
            break;

        return true;
    }

    public void Dispose()
    {
        foreach (var publisherTask in _publishers.Values)
        {
            if (!publisherTask.IsCompletedSuccessfully)
                continue;

            try
            {
                publisherTask.Result
                    .ShutdownAsync(CancellationToken.None)
                    .WaitAsync(TimeSpan.FromSeconds(5))
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Error shutting down a Pub/Sub publisher.");
            }
        }
    }
}
