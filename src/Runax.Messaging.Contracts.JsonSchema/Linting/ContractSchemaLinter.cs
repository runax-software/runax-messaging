using System.Text;
using System.Text.Json;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Checks committed contract schema files against the contract profile: a draft 2020-12 <c>$schema</c>, an
/// absolute <c>$id</c> that matches the file path, only profile keywords, an open root object, and unique
/// <c>$id</c>s. It runs the same schema compiler the runtime does, so it rejects exactly what
/// <see cref="ContractSchemaDocument.Parse"/> rejects.
/// <para>
/// Assert over this from your test suite, so a schema that the runtime could not load, or a typo that a
/// validator would silently ignore, fails before it is deployed:
/// </para>
/// <example>
/// <code>
/// ContractSchemaLinter.Lint("schemas").Errors.ShouldBeEmpty();
/// </code>
/// </example>
/// </summary>
public static class ContractSchemaLinter
{
    internal const string Draft202012 = "https://json-schema.org/draft/2020-12/schema";

    private const int MaxIdBytes = 256;

    /// <summary>
    /// Lints every <c>*.schema.json</c> file under <paramref name="directory"/>, recursively. The paths in
    /// the findings are relative to it, and the <c>$id</c> of each document has to match its path.
    /// </summary>
    /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
    public static ContractSchemaReport Lint(string directory) =>
        new(Lint(SchemaFile.FindAll(directory)));

    internal static List<ContractSchemaFinding> Lint(IReadOnlyList<SchemaFile> files)
    {
        var findings = new List<ContractSchemaFinding>();
        var loaded = new List<(SchemaFile File, ContractSchemaDocument Document)>();

        foreach (var file in files)
        {
            if (LintFile(file, findings) is { } document)
                loaded.Add((file, document));
        }

        LintSet(loaded, findings);
        return findings;
    }

    private static ContractSchemaDocument? LintFile(SchemaFile file, List<ContractSchemaFinding> findings)
    {
        using var parsed = file.TryParse(findings);
        if (parsed is null)
            return null;

        var root = parsed.RootElement;
        if (root.ValueKind == JsonValueKind.Object)
        {
            if (!root.TryGetProperty("$schema", out var draft) || draft.GetString() != Draft202012)
                Add(findings, file, "", ContractSchemaSeverity.Error, $"'$schema' must be \"{Draft202012}\".");

            if (root.TryGetProperty("additionalProperties", out var additional) && additional.ValueKind == JsonValueKind.False)
            {
                Add(findings, file, "/additionalProperties", ContractSchemaSeverity.Error,
                    "the root object must stay open: Runax messages carry '__runax' next to the payload, and readers " +
                    "must ignore properties newer writers add.");
            }

            WalkKeywords(file, root, "", findings);
        }

        var document = file.TryLoad(findings);
        if (document is null)
            return null;

        if (Encoding.UTF8.GetByteCount(document.Id) > MaxIdBytes)
            Add(findings, file, "/$id", ContractSchemaSeverity.Error, $"the $id is longer than {MaxIdBytes} bytes.");

        var expected = ExpectedPath(document.Id);
        var actual = file.RelativePath[..^".schema.json".Length];
        if (expected is null || !SegmentsEndWith(expected, actual.Split('/')))
        {
            Add(findings, file, "/$id", ContractSchemaSeverity.Error,
                $"the file path must match the $id '{document.Id}': expected the $id to end with '/{actual}'.");
        }

        if (!document.IsSelective)
        {
            Add(findings, file, "", ContractSchemaSeverity.Warning,
                "the schema accepts any JSON object, so it can't route messages that carry no contract identity. " +
                "Require a property, such as a type discriminator.");
        }

        return document;
    }

    private static void LintSet(List<(SchemaFile File, ContractSchemaDocument Document)> loaded, List<ContractSchemaFinding> findings)
    {
        foreach (var duplicate in loaded.GroupBy(x => x.Document.Id, StringComparer.Ordinal).Where(g => g.Count() > 1))
        {
            foreach (var (file, _) in duplicate.Skip(1))
            {
                Add(findings, file, "/$id", ContractSchemaSeverity.Error,
                    $"'{duplicate.Key}' is already declared by {duplicate.First().File.RelativePath}.");
            }
        }
    }

    // Unknown keywords would be silently ignored by validators; a typo like "requried" must not pass.
    private static void WalkKeywords(SchemaFile file, JsonElement schema, string pointer, List<ContractSchemaFinding> findings)
    {
        if (schema.ValueKind != JsonValueKind.Object)
            return;

        foreach (var keyword in schema.EnumerateObject())
        {
            var name = keyword.Name;
            var at = $"{pointer}/{Escape(name)}";

            switch (name)
            {
                case "properties" or "$defs" or "definitions" when keyword.Value.ValueKind == JsonValueKind.Object:
                    foreach (var child in keyword.Value.EnumerateObject())
                        WalkKeywords(file, child.Value, $"{at}/{Escape(child.Name)}", findings);
                    continue;
                case "items" or "additionalProperties":
                    WalkKeywords(file, keyword.Value, at, findings);
                    continue;
                case "anyOf" or "oneOf" when keyword.Value.ValueKind == JsonValueKind.Array:
                    var index = 0;
                    foreach (var branch in keyword.Value.EnumerateArray())
                        WalkKeywords(file, branch, $"{at}/{index++}", findings);
                    continue;
                case "format" when keyword.Value.ValueKind == JsonValueKind.String
                                   && !SchemaCompiler.AssertedFormats.Contains(keyword.Value.GetString()!):
                    Add(findings, file, at, ContractSchemaSeverity.Warning,
                        $"format '{keyword.Value.GetString()}' isn't checked; only " +
                        $"{string.Join(", ", SchemaCompiler.AssertedFormats)} are asserted.");
                    continue;
            }

            var known = SchemaCompiler.Evaluated.Contains(name)
                        || SchemaCompiler.Annotations.Contains(name)
                        || SchemaCompiler.Unsupported.Contains(name) // reported when the document is loaded
                        || name.StartsWith("x-", StringComparison.Ordinal);
            if (!known)
            {
                Add(findings, file, at, ContractSchemaSeverity.Error,
                    $"unknown keyword '{name}'. Custom annotations must start with 'x-'.");
            }
        }
    }

    /// <summary>The path segments an $id names, which the file path has to end with.</summary>
    internal static string[]? ExpectedPath(string id)
    {
        if (!Uri.TryCreate(id, UriKind.Absolute, out var uri))
            return null;

        var path = uri.Scheme is "http" or "https" ? uri.AbsolutePath : id[(uri.Scheme.Length + 1)..];
        return path.Split(['/', ':'], StringSplitOptions.RemoveEmptyEntries);
    }

    private static bool SegmentsEndWith(string[] segments, string[] suffix) =>
        segments.Length >= suffix.Length
        && segments.AsSpan(segments.Length - suffix.Length).SequenceEqual(suffix);

    private static void Add(List<ContractSchemaFinding> findings, SchemaFile file, string pointer, ContractSchemaSeverity severity, string message) =>
        findings.Add(new ContractSchemaFinding(file.RelativePath, pointer, severity, message));

    private static string Escape(string name) =>
        name.Replace("~", "~0", StringComparison.Ordinal).Replace("/", "~1", StringComparison.Ordinal);
}
