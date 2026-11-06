using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;

namespace Runax.Messaging;

/// <summary>
/// A topic and a contract a registered consumer handles. Exactly one identity is set: <see cref="Schema"/>
/// for a contract identified by its schema, <see cref="Version"/> for one identified by an integer, and
/// neither for an unversioned (accept-all) consumer.
/// </summary>
public sealed record HandledContract(string Topic, int? Version = null)
{
    /// <summary>
    /// Gets the contract schema <c>$id</c> handled, including lines a consumer's type <c>Reads</c>;
    /// <see langword="null"/> for a consumer without a schema.
    /// </summary>
    public string? Schema { get; init; }
}

/// <summary>
/// Introspects which topics and contracts this application consumes, so you can verify coverage at startup
/// (e.g. before letting a producer emit a new contract version or schema line).
/// </summary>
public interface IMessageContractCatalog
{
    /// <summary>
    /// Gets the distinct contracts handled by registered consumers: one entry per (topic, version) for
    /// consumers identified by an integer, and one per (topic, schema line) for consumers with schemas.
    /// </summary>
    IReadOnlyCollection<HandledContract> Handled { get; }

    /// <summary>
    /// Returns whether a consumer would receive a message of <paramref name="version"/> on
    /// <paramref name="topic"/> — true if a matching versioned consumer or an unversioned (accept-all)
    /// consumer is registered for that topic.
    /// </summary>
    bool Accepts(string topic, int version);

    /// <summary>
    /// Returns whether a consumer would receive a message carrying contract schema <paramref name="schema"/>
    /// on <paramref name="topic"/>.
    /// </summary>
    bool Accepts(string topic, string schema);

    /// <summary>
    /// Returns whether a consumer would receive a message on <paramref name="topic"/> carrying the given
    /// contract identity, by the same rule the dispatcher uses. A message without identity is routed by its
    /// body, which this can't see, so it only counts unversioned consumers for it.
    /// </summary>
    bool Accepts(string topic, string? schema, int? version);
}

internal sealed class MessageContractCatalog(
    IServiceProvider serviceProvider,
    IEnumerable<ConsumerRegistration> registrations) : IMessageContractCatalog
{
    private readonly Lazy<Entry[]> _entries = new(() => BuildEntries(serviceProvider, registrations));
    private IReadOnlyCollection<HandledContract>? _handled;

    public IReadOnlyCollection<HandledContract> Handled => _handled ??= BuildHandled(_entries.Value);

    public bool Accepts(string topic, int version) => Accepts(topic, null, version);

    public bool Accepts(string topic, string schema) => Accepts(topic, schema, null);

    public bool Accepts(string topic, string? schema, int? version)
    {
        var wireSchema = ContractSchemaId.NormalizeOrNull(schema);
        return _entries.Value.Any(e => e.Topic == topic && e.Accepts(wireSchema, version));
    }

    private static Entry[] BuildEntries(IServiceProvider serviceProvider, IEnumerable<ConsumerRegistration> registrations)
    {
        var entries = new List<Entry>();
        foreach (var registration in registrations)
        {
            var consumer = (IMessageConsumer)serviceProvider.GetRequiredService(registration.ConsumerType);
            entries.Add(new Entry(consumer.Topic, consumer.ContractVersion, consumer.ContractSchemas));
        }

        return [.. entries];
    }

    private static HandledContract[] BuildHandled(Entry[] entries)
    {
        var handled = new HashSet<HandledContract>();
        foreach (var entry in entries)
        {
            if (entry.Schemas.Count == 0)
            {
                handled.Add(new HandledContract(entry.Topic, entry.Version));
                continue;
            }

            foreach (var line in entry.Schemas)
                handled.Add(new HandledContract(entry.Topic) { Schema = line });
        }

        return [.. handled];
    }

    private sealed record Entry(string Topic, int? Version, IReadOnlySet<string> Schemas)
    {
        // The dispatcher's rule for an identified message (see ContractRouter). Routing an unidentified
        // message needs its body, which the catalog doesn't have, so only accept-all consumers count there.
        public bool Accepts(string? wireSchema, int? wireVersion)
        {
            if (Schemas.Count == 0 && Version is null)
                return true;

            if (Schemas.Count > 0)
                return wireSchema is not null && Schemas.Contains(wireSchema);

            return wireVersion == Version;
        }
    }
}
