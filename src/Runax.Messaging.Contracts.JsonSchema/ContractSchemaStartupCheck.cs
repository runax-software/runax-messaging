using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Consumers;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Warns at startup about contract schemas that won't behave as expected, before any message arrives:
/// lines a consumer declares but no loaded document covers, lines that accept any JSON object (and so can't
/// route unidentified messages), and lines on one topic that could both accept the same unidentified message
/// while reaching different consumers.
/// </summary>
internal sealed class ContractSchemaStartupCheck(
    IServiceProvider services,
    IEnumerable<ConsumerRegistration> registrations,
    IContractSchemaProvider provider,
    ILogger<ContractSchemaStartupCheck> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var topics = registrations
            .Select(r => (r.Bus, Consumer: (IMessageConsumer)services.GetRequiredService(r.ConsumerType)))
            .GroupBy(x => (x.Bus, x.Consumer.Topic));

        foreach (var topic in topics)
        {
            var consumers = topic.Select(x => x.Consumer).ToArray();
            var lines = consumers.SelectMany(c => c.ContractSchemas).Distinct(StringComparer.Ordinal).ToArray();
            var loaded = new List<ContractSchemaDocument>();

            foreach (var line in lines)
            {
                if (!provider.TryGet(line, out var document))
                {
                    logger.LogWarning(
                        "A consumer on topic '{Topic}' (bus '{Bus}') declares contract schema '{Schema}', but no " +
                        "loaded document has that $id; it isn't validated and can't route unidentified messages.",
                        topic.Key.Topic, topic.Key.Bus, line);
                }
                else if (!document.IsSelective)
                {
                    logger.LogWarning(
                        "Contract schema '{Schema}' on topic '{Topic}' (bus '{Bus}') accepts any JSON object, so it " +
                        "takes no part in routing messages without a contract identity. Require a property, such as " +
                        "a type discriminator, to make it selective.",
                        line, topic.Key.Topic, topic.Key.Bus);
                }
                else
                {
                    loaded.Add(document);
                }
            }

            var requiresIdentity = services.GetKeyedService<BusContractSettings>(topic.Key.Bus)?.RequireContractIdentity == true;
            if (!requiresIdentity)
                WarnAboutOverlaps(topic.Key.Bus, topic.Key.Topic, consumers, loaded);
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private void WarnAboutOverlaps(
        string bus,
        string topic,
        IMessageConsumer[] consumers,
        List<ContractSchemaDocument> lines)
    {
        for (var i = 0; i < lines.Count; i++)
        {
            for (var j = i + 1; j < lines.Count; j++)
            {
                var a = lines[i];
                var b = lines[j];
                var sameConsumers = consumers.All(c => c.ContractSchemas.Contains(a.Id) == c.ContractSchemas.Contains(b.Id));
                if (sameConsumers || ContractOverlap.AreDisjoint(a, b))
                    continue;

                logger.LogWarning(
                    "Contract schemas '{First}' and '{Second}' on topic '{Topic}' (bus '{Bus}') reach different consumers " +
                    "and could both accept the same message. An unidentified message matching both reaches neither. " +
                    "Add a required discriminator property with distinct values to tell them apart.",
                    a.Id, b.Id, topic, bus);
            }
        }
    }
}
