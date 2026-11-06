using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Decides which of a topic's consumers accept a message, from the contract identity it carries:
/// <list type="number">
///   <item><description>A schema <c>$id</c> decides when both the message and the consumer have one.</description></item>
///   <item><description>Otherwise the contract version decides, for contracts that have no schema.</description></item>
///   <item><description>A message with neither is routed by validating its body against the consumers'
///   schema lines, unless the bus requires identity. Lines that reach different consumers and both
///   accept the body make it ambiguous, and no schema consumer gets it.</description></item>
/// </list>
/// Unversioned consumers accept every message, as always.
/// </summary>
internal sealed class ContractRouter(IContractSchemaMatcher? matcher, BusContractSettings settings)
{
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
        foreach (var consumer in consumers)
        {
            if (Accepts(consumer, wireSchema, wireVersion, inferred))
                matched.Add(consumer);
        }

        var reason = (wireSchema, wireVersion, inference) switch
        {
            (null, null, Inference.Ambiguous) => UnroutableReason.AmbiguousSchema,
            (null, null, Inference.NoMatch) => UnroutableReason.NoMatchingSchema,
            _ => UnroutableReason.NoMatchingContract,
        };

        return new ContractRoute(matched, inferred, inference == Inference.Ambiguous, reason, detail);
    }

    /// <summary>
    /// Whether one consumer accepts the message. A consumer declares one identity or none: its schema
    /// lines, its integer version, or nothing at all.
    /// </summary>
    private static bool Accepts(
        IMessageConsumer consumer,
        string? wireSchema,
        int? wireVersion,
        IReadOnlyList<string> inferred)
    {
        var schemas = consumer.ContractSchemas;

        if (schemas.Count == 0 && consumer.ContractVersion is null)
            return true; // unversioned: accepts everything

        if (schemas.Count > 0)
        {
            // A mismatched $id is rejected even when a version matches: that's the drift schemas exist to
            // catch. A line being migrated is declared in Reads instead.
            return wireSchema is not null
                ? schemas.Contains(wireSchema)
                : wireVersion is null && inferred.Any(schemas.Contains);
        }

        return wireVersion == consumer.ContractVersion;
    }

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
/// <param name="Reason">Why the message is unroutable, when <paramref name="Matched"/> is empty.</param>
/// <param name="Detail">The validation failures or the ambiguous lines, for logs and the dead-letter reason.</param>
internal sealed record ContractRoute(
    List<IMessageConsumer> Matched,
    IReadOnlyList<string> Inferred,
    bool Ambiguous,
    UnroutableReason Reason,
    string? Detail);
