using System.Text.Json;
using Runax.Messaging.Contracts.JsonSchema;

namespace Runax.Messaging.Cli.Contracts;

/// <summary>
/// Compares two revisions of one contract line for <b>full</b> compatibility: readers of either accept what
/// the other writes, given that readers ignore unknown properties. In practice that leaves one freedom: an
/// optional property may be added or removed (on an open object). Every other structural difference is
/// breaking, except a changed <c>default</c>, which is a warning. References are compared after resolution,
/// so restructuring <c>$defs</c> is fine.
/// </summary>
internal sealed class CompatibilityChecker
{
    private readonly List<(string Pointer, Severity Severity, string Message)> _findings = [];
    private readonly HashSet<(SchemaNode, SchemaNode)> _visited = [];

    private CompatibilityChecker()
    {
    }

    /// <summary>Returns every difference between <paramref name="earlier"/> and <paramref name="next"/>.</summary>
    public static IReadOnlyList<(string Pointer, Severity Severity, string Message)> Compare(
        ContractSchemaDocument earlier,
        ContractSchemaDocument next)
    {
        var checker = new CompatibilityChecker();
        checker.CompareNodes(earlier.Root, next.Root, "");
        return checker._findings;
    }

    private void CompareNodes(SchemaNode earlier, SchemaNode next, string pointer)
    {
        earlier = Resolve(earlier);
        next = Resolve(next);

        // Recursive schemas: a pair already under comparison is assumed equal (coinductively).
        if (!_visited.Add((earlier, next)))
            return;

        if (earlier.Rejects != next.Rejects)
        {
            Breaking(pointer, next.Rejects ? "the schema now rejects every value" : "the schema no longer rejects every value");
            return;
        }

        if (earlier.Ref is not null && next.Ref is not null)
            CompareNodes(earlier.Ref, next.Ref, $"{pointer}/$ref");
        else if ((earlier.Ref is null) != (next.Ref is null))
            Breaking(pointer, "a $ref was added or removed alongside other keywords");

        CompareTypes(earlier.Types, next.Types, pointer);
        CompareValues(earlier, next, pointer);
        CompareScalar(earlier.Format, next.Format, pointer, "format");
        CompareScalar(earlier.Minimum, next.Minimum, pointer, "minimum");
        CompareScalar(earlier.Maximum, next.Maximum, pointer, "maximum");
        CompareScalar(earlier.ExclusiveMinimum, next.ExclusiveMinimum, pointer, "exclusiveMinimum");
        CompareScalar(earlier.ExclusiveMaximum, next.ExclusiveMaximum, pointer, "exclusiveMaximum");
        CompareScalar(earlier.MultipleOf, next.MultipleOf, pointer, "multipleOf");
        CompareScalar(earlier.MinLength, next.MinLength, pointer, "minLength");
        CompareScalar(earlier.MaxLength, next.MaxLength, pointer, "maxLength");
        CompareScalar(earlier.Pattern?.ToString(), next.Pattern?.ToString(), pointer, "pattern");
        CompareScalar(earlier.MinItems, next.MinItems, pointer, "minItems");
        CompareScalar(earlier.MaxItems, next.MaxItems, pointer, "maxItems");
        CompareScalar((bool?)earlier.UniqueItems, (bool?)next.UniqueItems, pointer, "uniqueItems");
        CompareScalar(earlier.MinProperties, next.MinProperties, pointer, "minProperties");
        CompareScalar(earlier.MaxProperties, next.MaxProperties, pointer, "maxProperties");
        CompareOptional(earlier.Items, next.Items, $"{pointer}/items", "items");
        CompareAdditionalProperties(earlier.AdditionalProperties, next.AdditionalProperties, pointer);
        CompareProperties(earlier, next, pointer);
        CompareBranches(earlier.AnyOf, next.AnyOf, pointer, "anyOf");
        CompareBranches(earlier.OneOf, next.OneOf, pointer, "oneOf");

        if (!JsonEquals(earlier.Default, next.Default))
            _findings.Add((pointer, Severity.Warning, "the default changed: same structure, different meaning"));
    }

    private void CompareTypes(JsonTypes earlier, JsonTypes next, string pointer)
    {
        if (earlier == next)
            return;

        var change = earlier ^ next;
        var message = change == JsonTypes.Null
            ? next.HasFlag(JsonTypes.Null) ? "null is now allowed" : "null is no longer allowed"
            : $"the type changed from {Describe(earlier)} to {Describe(next)}";
        Breaking(pointer, message);
    }

    private void CompareValues(SchemaNode earlier, SchemaNode next, string pointer)
    {
        if (!JsonEquals(earlier.Const, next.Const))
            Breaking(pointer, $"const changed from {Raw(earlier.Const)} to {Raw(next.Const)}");

        if (earlier.Enum is null && next.Enum is null)
            return;

        var before = earlier.Enum ?? [];
        var after = next.Enum ?? [];
        var added = after.Where(v => !before.Any(w => JsonElement.DeepEquals(v, w))).Select(v => v.GetRawText()).ToArray();
        var removed = before.Where(v => !after.Any(w => JsonElement.DeepEquals(v, w))).Select(v => v.GetRawText()).ToArray();
        if (earlier.Enum is null || next.Enum is null)
            Breaking(pointer, earlier.Enum is null ? "an enum was added" : "the enum was removed");
        else if (added.Length > 0 || removed.Length > 0)
            Breaking(pointer, $"enum values changed (added: {List(added)}; removed: {List(removed)})");
    }

    private void CompareScalar<T>(T? earlier, T? next, string pointer, string keyword)
    {
        if (EqualityComparer<T?>.Default.Equals(earlier, next))
            return;

        Breaking(pointer, (earlier, next) switch
        {
            (null, _) => $"{keyword} {Show(next)} was added",
            (_, null) => $"{keyword} {Show(earlier)} was removed",
            _ => $"{keyword} changed from {Show(earlier)} to {Show(next)}",
        });
    }

    private void CompareOptional(SchemaNode? earlier, SchemaNode? next, string pointer, string keyword)
    {
        if (earlier is null && next is null)
            return;

        if (earlier is null || next is null)
            Breaking(pointer, earlier is null ? $"{keyword} was added" : $"{keyword} was removed");
        else
            CompareNodes(earlier, next, pointer);
    }

    // Absent and `true` both mean an open object.
    private void CompareAdditionalProperties(SchemaNode? earlier, SchemaNode? next, string pointer)
    {
        var before = IsOpen(earlier) ? null : earlier;
        var after = IsOpen(next) ? null : next;
        if (before is null && after is null)
            return;

        if (before is null || after is null)
        {
            Breaking($"{pointer}/additionalProperties", before is null
                ? "the object was closed or restricted: readers would reject properties writers may add"
                : "the object was opened: earlier readers reject properties new writers may add");
            return;
        }

        CompareNodes(before, after, $"{pointer}/additionalProperties");
    }

    private void CompareProperties(SchemaNode earlier, SchemaNode next, string pointer)
    {
        var requiredBefore = new HashSet<string>(earlier.Required ?? [], StringComparer.Ordinal);
        var requiredAfter = new HashSet<string>(next.Required ?? [], StringComparer.Ordinal);
        var propertiesBefore = earlier.Properties ?? [];
        var propertiesAfter = next.Properties ?? [];

        var names = propertiesBefore.Keys.Concat(propertiesAfter.Keys).Concat(requiredBefore).Concat(requiredAfter)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal);

        foreach (var name in names)
        {
            var at = $"{pointer}/properties/{Escape(name)}";
            var wasDeclared = propertiesBefore.TryGetValue(name, out var before);
            var isDeclared = propertiesAfter.TryGetValue(name, out var after);
            var wasRequired = requiredBefore.Contains(name);
            var isRequired = requiredAfter.Contains(name);

            if (wasRequired && !isRequired)
                Breaking(at, isDeclared ? $"required property '{name}' was made optional" : $"required property '{name}' was removed");
            else if (!wasRequired && isRequired)
                Breaking(at, wasDeclared ? $"optional property '{name}' was made required" : $"required property '{name}' was added");

            if (wasDeclared && isDeclared)
            {
                CompareNodes(before!, after!, at);
            }
            else if (isDeclared && !isRequired)
            {
                // An optional property added: earlier readers must accept it under their additionalProperties.
                CheckAgainstAdditional(earlier.AdditionalProperties, after!, at, $"optional property '{name}' was added to a closed object");
            }
            else if (wasDeclared && !wasRequired)
            {
                // An optional property removed: earlier writers still send it, and new readers must accept it.
                CheckAgainstAdditional(next.AdditionalProperties, before!, at, $"optional property '{name}' was removed from a closed object");
            }
        }
    }

    private void CheckAgainstAdditional(SchemaNode? additional, SchemaNode property, string pointer, string closedMessage)
    {
        if (IsOpen(additional))
            return;

        if (additional!.Rejects)
            Breaking(pointer, closedMessage);
        else
            CompareNodes(additional, property, pointer);
    }

    // Branches match regardless of order; any branch without an equivalent on the other side is breaking.
    private void CompareBranches(SchemaNode[]? earlier, SchemaNode[]? next, string pointer, string keyword)
    {
        if (earlier is null && next is null)
            return;

        if (earlier is null || next is null)
        {
            Breaking($"{pointer}/{keyword}", earlier is null ? $"{keyword} was added" : $"{keyword} was removed");
            return;
        }

        for (var i = 0; i < earlier.Length; i++)
        {
            if (!next.Any(branch => AreEquivalent(earlier[i], branch)))
                Breaking($"{pointer}/{keyword}/{i}", $"{keyword} branch {i} was removed or changed");
        }

        for (var i = 0; i < next.Length; i++)
        {
            if (!earlier.Any(branch => AreEquivalent(branch, next[i])))
                Breaking($"{pointer}/{keyword}/{i}", $"{keyword} branch {i} was added or changed");
        }
    }

    private bool AreEquivalent(SchemaNode a, SchemaNode b)
    {
        var probe = new CompatibilityChecker();
        foreach (var pair in _visited)
            probe._visited.Add(pair);

        probe.CompareNodes(a, b, "");
        return probe._findings.All(f => f.Severity == Severity.Warning);
    }

    private void Breaking(string pointer, string message) => _findings.Add((pointer, Severity.Error, message));

    private static bool IsOpen(SchemaNode? additional) =>
        additional is null || ReferenceEquals(additional, SchemaNode.AlwaysValid);

    // A node that is nothing but a $ref is its target.
    private static SchemaNode Resolve(SchemaNode node)
    {
        var seen = new HashSet<SchemaNode>();
        while (node.Ref is { } target && IsOnlyRef(node) && seen.Add(node))
            node = target;
        return node;
    }

    private static bool IsOnlyRef(SchemaNode node) =>
        node.Types == JsonTypes.None && node.Properties is null && node.Required is null
        && node.AdditionalProperties is null && node.Items is null && node.Enum is null && node.Const is null
        && node.Format is null && node.AnyOf is null && node.OneOf is null && node.Minimum is null
        && node.Maximum is null && node.ExclusiveMinimum is null && node.ExclusiveMaximum is null
        && node.MultipleOf is null && node.MinLength is null && node.MaxLength is null && node.Pattern is null
        && node.MinItems is null && node.MaxItems is null && !node.UniqueItems && node.MinProperties is null
        && node.MaxProperties is null;

    private static bool JsonEquals(JsonElement? a, JsonElement? b) =>
        a is null ? b is null : b is not null && JsonElement.DeepEquals(a.Value, b.Value);

    private static string Describe(JsonTypes types) => types == JsonTypes.None
        ? "any"
        : string.Join(" | ", Enum.GetValues<JsonTypes>()
            .Where(t => t != JsonTypes.None && types.HasFlag(t))
            .Select(t => t.ToString().ToLowerInvariant()));

    private static string Raw(JsonElement? value) => value?.GetRawText() ?? "(none)";

    private static string Show<T>(T value) => value switch
    {
        string text => $"'{text}'",
        bool flag => flag ? "true" : "false",
        _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "",
    };

    private static string List(string[] values) => values.Length == 0 ? "none" : string.Join(", ", values);

    private static string Escape(string name) =>
        name.Replace("~", "~0", StringComparison.Ordinal).Replace("/", "~1", StringComparison.Ordinal);
}
