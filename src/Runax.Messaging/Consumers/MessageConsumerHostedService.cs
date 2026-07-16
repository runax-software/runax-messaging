using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Background service that subscribes registered message consumers to their topics
/// and dispatches incoming messages.
/// </summary>
internal sealed class MessageConsumerHostedService(
    IServiceProvider serviceProvider,
    IEnumerable<ConsumerRegistration> registrations,
    IMessagingTransport transport,
    IMessageSerializer serializer,
    ILogger<MessageConsumerHostedService> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topicConsumers = new Dictionary<string, List<IMessageConsumer>>();
        foreach (var registration in registrations)
        {
            var consumer = (IMessageConsumer)serviceProvider.GetRequiredService(registration.ConsumerType);

            if (!topicConsumers.TryGetValue(consumer.Topic, out var list))
            {
                list = [];
                topicConsumers[consumer.Topic] = list;
            }

            list.Add(consumer);
        }

        var allTopics = topicConsumers.Keys.ToArray();
        if (allTopics.Length == 0)
        {
            logger.LogInformation("No topics to subscribe to. No consumers registered any topics.");
            return;
        }

        logger.LogInformation("Subscribing to {TopicCount} topic(s): {Topics}", allTopics.Length,
            string.Join(", ", allTopics));

        await transport.SubscribeAsync(allTopics, async (envelopeJson, topic) =>
        {
            if (!topicConsumers.TryGetValue(topic, out var consumers))
                return;

            var context = serializer.Deserialize(envelopeJson, topic);

            foreach (var consumer in consumers)
            {
                try
                {
                    await consumer.HandleAsync(context, stoppingToken);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex,
                        "Consumer {ConsumerType} failed to handle message on topic '{Topic}'.",
                        consumer.GetType().Name, topic);
                }
            }
        }, stoppingToken);
    }
}
