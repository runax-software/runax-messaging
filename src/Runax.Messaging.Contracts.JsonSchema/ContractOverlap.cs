using System.Text.Json;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Decides, conservatively, whether two contract lines can never accept the same message. General schema
/// overlap is hard to decide, so two lines count as disjoint only when both require a top-level property
/// whose <c>const</c>, <c>enum</c>, or <c>type</c> values don't intersect, such as a type discriminator.
/// </summary>
internal static class ContractOverlap
{
    public static bool AreDisjoint(ContractSchemaDocument a, ContractSchemaDocument b)
    {
        var left = Resolve(a.Root);
        var right = Resolve(b.Root);
        if (left.Required is null || right.Required is null || left.Properties is null || right.Properties is null)
            return false;

        foreach (var name in left.Required.Intersect(right.Required, StringComparer.Ordinal))
        {
            if (left.Properties.TryGetValue(name, out var x)
                && right.Properties.TryGetValue(name, out var y)
                && Disjoint(Resolve(x), Resolve(y)))
            {
                return true;
            }
        }

        return false;
    }

    private static bool Disjoint(SchemaNode x, SchemaNode y)
    {
        if (Values(x) is { } xs && Values(y) is { } ys)
            return !xs.Any(v => ys.Any(w => JsonElement.DeepEquals(v, w)));

        if (x.Types != JsonTypes.None && y.Types != JsonTypes.None)
            return (Widen(x.Types) & Widen(y.Types)) == JsonTypes.None;

        return false;
    }

    private static JsonElement[]? Values(SchemaNode node) =>
        node.Const is { } constant ? [constant] : node.Enum;

    // An integer is also a number, so the two overlap.
    private static JsonTypes Widen(JsonTypes types) =>
        types.HasFlag(JsonTypes.Integer) ? types | JsonTypes.Number : types;

    private static SchemaNode Resolve(SchemaNode node) =>
        node.Ref is { } target && node.Properties is null && node.Const is null && node.Enum is null ? target : node;
}
