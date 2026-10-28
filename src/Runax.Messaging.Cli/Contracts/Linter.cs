using System.Text;
using System.Text.Json;
using Runax.Messaging.Contracts.JsonSchema;

namespace Runax.Messaging.Cli.Contracts;

/// <summary>
/// <c>runax-messaging contracts lint</c>: checks each schema file on its own, and the set of files together, against
/// the contract profile. The runtime loads exactly what passes.
/// </summary>
internal static class Linter
{
    public const string Draft202012 = "https://json-schema.org/draft/2020-12/schema";

    private const int MaxIdBytes = 256;

    public static List<Finding> Lint(IReadOnlyList<SchemaFile> files)
    {
        var findings = new List<Finding>();
        var loaded = new List<(SchemaFile File, ContractSchemaDocument Document)>();

        foreach (var file in files)
        {
            if (LintFile(file, findings) is { } document)
                loaded.Add((file, document));
        }

        LintSet(loaded, findings);
        return findings;
    }

    private static ContractSchemaDocument? LintFile(SchemaFile file, List<Finding> findings)
    {
        using var parsed = file.TryParse(findings);
        if (parsed is null)
            return null;

        var root = parsed.RootElement;
        if (root.ValueKind == JsonValueKind.Object)
        {
            if (!root.TryGetProperty("$schema", out var draft) || draft.GetString() != Draft202012)
                Add(findings, file, "", Severity.Error, $"'$schema' must be \"{Draft202012}\".");

            if (root.TryGetProperty("additionalProperties", out var additional) && additional.ValueKind == JsonValueKind.False)
            {
                Add(findings, file, "/additionalProperties", Severity.Error,
                    "the root object must stay open: Runax messages carry '__runax' next to the payload, and readers " +
                    "must ignore properties newer writers add.");
            }

            WalkKeywords(file, root, "", findings);
        }

        var document = file.TryLoad(findings);
        if (document is null)
            return null;

        if (Encoding.UTF8.GetByteCount(document.Id) > MaxIdBytes)
            Add(findings, file, "/$id", Severity.Error, $"the $id is longer than {MaxIdBytes} bytes.");

        var expected = ExpectedPath(document.Id);
        var actual = file.RelativePath[..^".schema.json".Length];
        if (expected is null || !SegmentsEndWith(expected, actual.Split('/')))
        {
            Add(findings, file, "/$id", Severity.Error,
                $"the file path must match the $id '{document.Id}': expected the $id to end with '/{actual}'.");
        }

        if (!document.IsSelective)
        {
            Add(findings, file, "", Severity.Warning,
                "the schema accepts any JSON object, so it can't route messages that carry no contract identity. " +
                "Require a property, such as a type discriminator.");
        }

        return document;
    }

    private static void LintSet(List<(SchemaFile File, ContractSchemaDocument Document)> loaded, List<Finding> findings)
    {
        foreach (var duplicate in loaded.GroupBy(x => x.Document.Id, StringComparer.Ordinal).Where(g => g.Count() > 1))
        {
            foreach (var (file, _) in duplicate.Skip(1))
            {
                Add(findings, file, "/$id", Severity.Error,
                    $"'{duplicate.Key}' is already declared by {duplicate.First().File.RelativePath}.");
            }
        }

        // Each breaking line of one contract needs its own legacy integer, or 2.x consumers routing by the
        // integer would receive the new shape.
        var lines = loaded
            .Where(x => x.Document.LegacyVersion is not null)
            .GroupBy(x => (Contract: ContractOf(x.Document.Id), Version: x.Document.LegacyVersion!.Value));
        foreach (var shared in lines.Where(g => g.Select(x => x.Document.Id).Distinct(StringComparer.Ordinal).Count() > 1))
        {
            foreach (var (file, document) in shared.Skip(1))
            {
                Add(findings, file, "/x-runax-legacy-version", Severity.Error,
                    $"legacy version {shared.Key.Version} is already used by '{shared.First().Document.Id}'. A breaking " +
                    "change mints a new $id and a new integer together.");
            }
        }
    }

    // Unknown keywords would be silently ignored by validators; a typo like "requried" must not pass.
    private static void WalkKeywords(SchemaFile file, JsonElement schema, string pointer, List<Finding> findings)
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
                    Add(findings, file, at, Severity.Warning,
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
                Add(findings, file, at, Severity.Error,
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

    /// <summary>The contract an $id belongs to: the $id without its last (version) segment.</summary>
    internal static string ContractOf(string id)
    {
        var cut = id.LastIndexOfAny(['/', ':']);
        return cut > 0 ? id[..cut] : id;
    }

    private static bool SegmentsEndWith(string[] segments, string[] suffix) =>
        segments.Length >= suffix.Length
        && segments.AsSpan(segments.Length - suffix.Length).SequenceEqual(suffix);

    private static void Add(List<Finding> findings, SchemaFile file, string pointer, Severity severity, string message) =>
        findings.Add(new Finding(file.RelativePath, pointer, severity, message));

    private static string Escape(string name) =>
        name.Replace("~", "~0", StringComparison.Ordinal).Replace("/", "~1", StringComparison.Ordinal);
}
