namespace Runax.Messaging;

/// <summary>
/// Contract schema <c>$id</c>s are compared by ordinal string equality, after dropping an empty trailing
/// fragment (<c>#</c>). There is no case folding or other URI normalization.
/// </summary>
internal static class ContractSchemaId
{
    /// <summary>Returns <paramref name="id"/> without an empty trailing fragment.</summary>
    public static string Normalize(string id) => id.EndsWith('#') ? id[..^1] : id;

    /// <summary>Returns the normalized id, or <see langword="null"/> for a missing or blank value.</summary>
    public static string? NormalizeOrNull(string? id) => string.IsNullOrWhiteSpace(id) ? null : Normalize(id);
}
