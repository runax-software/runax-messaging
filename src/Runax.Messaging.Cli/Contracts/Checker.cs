using Runax.Messaging.Contracts.JsonSchema;

namespace Runax.Messaging.Cli.Contracts;

/// <summary>
/// <c>runax-messaging contracts check</c>: every schema file must be fully compatible with every earlier revision that
/// declared the same <c>$id</c>, and must keep the same <c>x-runax-legacy-version</c>. Earlier revisions come
/// from git history, or from a baseline directory (for example a checkout of the base branch).
/// </summary>
internal static class Checker
{
    /// <summary>Compares each file with its revisions in git history.</summary>
    public static List<Finding> CheckHistory(IReadOnlyList<SchemaFile> files, GitHistory history)
    {
        var findings = new List<Finding>();
        foreach (var file in files)
        {
            var current = file.TryLoad(findings);
            if (current is null)
                continue;

            var compared = new HashSet<string>(StringComparer.Ordinal) { file.Json };
            foreach (var (commit, json) in history.Revisions(file.FullPath))
            {
                // Identical content was already compared; the working copy often equals the latest commit.
                if (!compared.Add(json))
                    continue;

                CompareRevision(file, current, json, $"commit {commit[..Math.Min(12, commit.Length)]}", findings);
            }
        }

        return findings;
    }

    /// <summary>Compares each file with the file declaring the same $id in <paramref name="baseline"/>.</summary>
    public static List<Finding> CheckBaseline(IReadOnlyList<SchemaFile> files, IReadOnlyList<SchemaFile> baseline)
    {
        var findings = new List<Finding>();
        var earlierById = new Dictionary<string, SchemaFile>(StringComparer.Ordinal);
        foreach (var file in baseline)
        {
            if (SchemaFile.TryLoad(file.Json, file.RelativePath, []) is { } document)
                earlierById.TryAdd(document.Id, file);
        }

        foreach (var file in files)
        {
            var current = file.TryLoad(findings);
            if (current is not null && earlierById.TryGetValue(current.Id, out var earlier) && earlier.Json != file.Json)
                CompareRevision(file, current, earlier.Json, $"baseline {earlier.RelativePath}", findings);
        }

        return findings;
    }

    private static void CompareRevision(
        SchemaFile file,
        ContractSchemaDocument current,
        string earlierJson,
        string revision,
        List<Finding> findings)
    {
        // A revision that no longer loads (or declared another $id) isn't part of this line's history.
        var earlier = SchemaFile.TryLoad(earlierJson, file.RelativePath, []);
        if (earlier is null || earlier.Id != current.Id)
            return;

        if (earlier.LegacyVersion != current.LegacyVersion)
        {
            findings.Add(new Finding(file.RelativePath, "/x-runax-legacy-version", Severity.Error,
                $"the legacy version changed from {Show(earlier.LegacyVersion)} to {Show(current.LegacyVersion)} " +
                $"since {revision}. It must never change within a $id."));
        }

        foreach (var (pointer, severity, message) in CompatibilityChecker.Compare(earlier, current))
        {
            var advice = severity == Severity.Error ? $" {Advice(current.Id)}" : "";
            findings.Add(new Finding(file.RelativePath, pointer, severity, $"{message} since {revision}.{advice}"));
        }
    }

    private static string Show(int? version) =>
        version?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "(none)";

    // "…/order-placed/v2" suggests "…/order-placed/v3".
    internal static string Advice(string id)
    {
        var cut = id.LastIndexOfAny(['/', ':']) + 1;
        var last = id[cut..];
        var next = last.Length > 1 && last[0] == 'v' && int.TryParse(last[1..], out var major)
            ? $"'{id[..cut]}v{major + 1}'"
            : "a new $id";
        return $"Breaking changes need a new line: mint {next} (with a new legacy version) and keep '{id}' unchanged " +
               "until its consumers are drained.";
    }
}
