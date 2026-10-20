using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;

namespace Runax.Messaging;

/// <summary>
/// A topic and a contract a registered consumer handles. <see cref="Version"/> is <see langword="null"/> for an
/// unversioned (accept-all) consumer, unless <see cref="Schema"/> is set: then it's the schema line's legacy
/// version when known.
/// </summary>
public sealed record HandledContract(string Topic, int? Version)
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
    /// consumers without a schema, and one per (topic, schema line) for consumers with one.
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
        var matcher = serviceProvider.GetService<IContractSchemaMatcher>();
        var entries = new List<Entry>();
        foreach (var registration in registrations)
        {
            var consumer = (IMessageConsumer)serviceProvider.GetRequiredService(registration.ConsumerType);
            var versions = new HashSet<int>();
            if (consumer.ContractVersion is { } version)
                versions.Add(version);
            foreach (var line in consumer.ContractSchemas)
            {
                if (matcher?.LegacyVersionOf(line) is { } legacy)
                    versions.Add(legacy);
            }

            entries.Add(new Entry(
                consumer.Topic, consumer.ContractVersion, consumer.ContractSchema, consumer.ContractSchemas, versions, matcher));
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
                handled.Add(new HandledContract(entry.Topic, entry.TypeVersion));
                continue;
            }

            foreach (var line in entry.Schemas)
            {
                var version = line == entry.TypeSchema ? entry.TypeVersion : entry.Matcher?.LegacyVersionOf(line);
                handled.Add(new HandledContract(entry.Topic, version) { Schema = line });
            }
        }

        return [.. handled];
    }

    private sealed record Entry(
        string Topic,
        int? TypeVersion,
        string? TypeSchema,
        IReadOnlySet<string> Schemas,
        IReadOnlySet<int> Versions,
        IContractSchemaMatcher? Matcher)
    {
        // The dispatcher's rule for an identified message (see ContractRouter), without the per-bus safety valve.
        public bool Accepts(string? wireSchema, int? wireVersion)
        {
            if (Schemas.Count == 0 && Versions.Count == 0)
                return true;

            if (wireSchema is not null && Schemas.Count > 0)
                return Schemas.Contains(wireSchema);

            return wireVersion is { } version && Versions.Contains(version);
        }
    }
}
