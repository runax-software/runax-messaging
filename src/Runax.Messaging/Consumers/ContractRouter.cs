using System.Collections.Concurrent;
using System.Collections.Frozen;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Decides which of a topic's consumers accept a message, from the contract identity it carries:
/// <list type="number">
///   <item><description>A schema <c>$id</c> decides when both the message and the consumer have one.</description></item>
///   <item><description>Otherwise the contract version decides. A consumer accepts its type's version plus
///   the legacy version each of its schema lines records.</description></item>
///   <item><description>A message with neither is routed by validating its body against the consumers'
///   schema lines, unless the bus requires identity. Lines that reach different consumers and both
///   accept the body make it ambiguous, and no schema consumer gets it.</description></item>
/// </list>
/// Unversioned consumers accept every message, as always.
/// </summary>
internal sealed class ContractRouter(IContractSchemaMatcher? matcher, BusContractSettings settings)
{
    private readonly ConcurrentDictionary<IMessageConsumer, FrozenSet<int>> _versions = new();

    public ContractRoute Route(IReadOnlyList<IMessageConsumer> consumers, MessageContext context)
    {
        var wireSchema = context.ContractSchema;
        var wireVersion = context.ContractVersion;

        IReadOnlyList<string> inferred = [];
        var inference = Inference.NotRun;
        string? detail = null;

        if (wireSchema is null && wireVersion is null && matcher is not null && !settings.RequireContractIdentity)
            (inferred, inference, detail) = Infer(consumers, context.Body);

        var matched = new List<IMessageConsumer>(consumers.Count);
        var mismatched = new List<IMessageConsumer>();
        foreach (var consumer in consumers)
        {
            switch (Accepts(consumer, wireSchema, wireVersion, inferred))
            {
                case Acceptance.Accepted:
                    matched.Add(consumer);
                    break;
                case Acceptance.AcceptedDespiteMismatch:
                    matched.Add(consumer);
                    mismatched.Add(consumer);
                    break;
            }
        }

        var reason = (wireSchema, wireVersion, inference) switch
        {
            (null, null, Inference.Ambiguous) => UnroutableReason.AmbiguousSchema,
            (null, null, Inference.NoMatch) => UnroutableReason.NoMatchingSchema,
            _ => UnroutableReason.NoMatchingContract,
        };

        return new ContractRoute(matched, inferred, inference == Inference.Ambiguous, mismatched, reason, detail);
    }

    private Acceptance Accepts(
        IMessageConsumer consumer,
        string? wireSchema,
        int? wireVersion,
        IReadOnlyList<string> inferred)
    {
        var schemas = consumer.ContractSchemas;
        var versions = VersionsOf(consumer);

        if (schemas.Count == 0 && versions.Count == 0)
            return Acceptance.Accepted; // unversioned: accepts everything

        if (wireSchema is not null && schemas.Count > 0)
        {
            if (schemas.Contains(wireSchema))
                return Acceptance.Accepted;

            // The safety valve: a mismatched $id still routes by a matching version when the bus opted in.
            return settings.Mismatch == SchemaMismatch.Report
                   && wireVersion is { } version
                   && versions.Contains(version)
                ? Acceptance.AcceptedDespiteMismatch
                : Acceptance.Rejected;
        }

        if (wireVersion is { } claimed)
            return versions.Contains(claimed) ? Acceptance.Accepted : Acceptance.Rejected;

        return wireSchema is null && inferred.Any(schemas.Contains) ? Acceptance.Accepted : Acceptance.Rejected;
    }

    // The consumer's type version, plus the legacy version recorded by each of its schema lines.
    private FrozenSet<int> VersionsOf(IMessageConsumer consumer) =>
        _versions.GetOrAdd(consumer, c =>
        {
            var versions = new HashSet<int>();
            if (c.ContractVersion is { } version)
                versions.Add(version);

            if (matcher is not null)
            {
                foreach (var schema in c.ContractSchemas)
                {
                    if (matcher.LegacyVersionOf(schema) is { } legacy)
                        versions.Add(legacy);
                }
            }

            return versions.ToFrozenSet();
        });

    private (IReadOnlyList<string> Inferred, Inference Outcome, string? Detail) Infer(
        IReadOnlyList<IMessageConsumer> consumers,
        string body)
    {
        var candidates = consumers.SelectMany(c => c.ContractSchemas).Distinct(StringComparer.Ordinal).ToArray();
        if (candidates.Length == 0)
            return ([], Inference.NotRun, null);

        var match = matcher!.Match(body, candidates);
        if (match.Accepted.Count == 0)
            return ([], Inference.NoMatch, string.Join("; ", match.Failures));

        // Ambiguous when two accepted lines reach different sets of consumers.
        var reach = match.Accepted
            .Select(line => consumers.Where(c => c.ContractSchemas.Contains(line)).ToHashSet())
            .ToArray();
        if (reach.Skip(1).Any(set => !set.SetEquals(reach[0])))
            return ([], Inference.Ambiguous, string.Join(", ", match.Accepted));

        return (match.Accepted, Inference.Matched, null);
    }

    private enum Acceptance
    {
        Rejected,
        Accepted,
        AcceptedDespiteMismatch,
    }

    private enum Inference
    {
        NotRun,
        Matched,
        NoMatch,
        Ambiguous,
    }
}

/// <summary>The outcome of <see cref="ContractRouter.Route"/>.</summary>
/// <param name="Matched">The consumers that accept the message.</param>
/// <param name="Inferred">The schema lines an unidentified message was routed by; empty otherwise.</param>
/// <param name="Ambiguous">Whether an unidentified message matched lines that reach different consumers.</param>
/// <param name="Mismatched">Consumers that accepted a mismatched <c>$id</c> through the safety valve.</param>
/// <param name="Reason">Why the message is unroutable, when <paramref name="Matched"/> is empty.</param>
/// <param name="Detail">The validation failures or the ambiguous lines, for logs and the dead-letter reason.</param>
internal sealed record ContractRoute(
    List<IMessageConsumer> Matched,
    IReadOnlyList<string> Inferred,
    bool Ambiguous,
    List<IMessageConsumer> Mismatched,
    UnroutableReason Reason,
    string? Detail);
