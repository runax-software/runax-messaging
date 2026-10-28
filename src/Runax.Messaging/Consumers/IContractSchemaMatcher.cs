namespace Runax.Messaging.Consumers;

/// <summary>
/// What routing needs from a JSON Schema evaluator, which core deliberately doesn't have:
/// the legacy integer each contract line records, and which lines an unidentified message's body
/// validates against. Implemented by <c>Runax.Messaging.Contracts.JsonSchema</c>; without it,
/// unidentified messages only reach unversioned consumers, as before.
/// </summary>
internal interface IContractSchemaMatcher
{
    /// <summary>
    /// Returns the line's <c>x-runax-legacy-version</c>, or <see langword="null"/> when the line's document
    /// is unknown or records none.
    /// </summary>
    int? LegacyVersionOf(string contractSchema);

    /// <summary>
    /// Evaluates an unidentified message's body against <paramref name="candidates"/>. Lines whose
    /// documents are unknown, or that aren't selective enough to route by, take no part.
    /// </summary>
    ContractMatch Match(string body, IReadOnlyCollection<string> candidates);
}

/// <summary>The outcome of <see cref="IContractSchemaMatcher.Match"/>.</summary>
/// <param name="Accepted">The candidate lines the body validated against.</param>
/// <param name="Failures">Why each rejecting line rejected it, e.g. "…/v2 at /currency: required".</param>
internal sealed record ContractMatch(IReadOnlyList<string> Accepted, IReadOnlyList<string> Failures)
{
    public static ContractMatch None { get; } = new([], []);
}
