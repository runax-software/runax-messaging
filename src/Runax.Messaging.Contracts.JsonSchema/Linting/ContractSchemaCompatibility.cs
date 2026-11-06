namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Checks that a contract line stayed compatible with its earlier revisions. Exact <c>$id</c> matching is
/// only safe while every revision published under one <c>$id</c> accepts what every earlier one writes, so
/// <c>…/v2</c> keeps meaning one shape. A running service can't check that — it only has the file it shipped
/// with — so check it before anything is deployed.
/// <para>
/// In practice one freedom is left: an optional property may be added or removed on an open object. Every
/// other structural difference is breaking, and the finding says which new <c>$id</c> to mint. A changed
/// <c>default</c> is a warning, and <c>x-runax-legacy-version</c> must never change within a <c>$id</c>.
/// </para>
/// <example>
/// <code>
/// // in a test, against a checkout of the base branch
/// ContractSchemaCompatibility.Compare("../.baseline/schemas", "schemas").Errors.ShouldBeEmpty();
/// </code>
/// </example>
/// </summary>
public static class ContractSchemaCompatibility
{
    /// <summary>
    /// Compares every schema file under <paramref name="directory"/> with the file declaring the same
    /// <c>$id</c> under <paramref name="baselineDirectory"/>. A line with no counterpart in the baseline is
    /// new and has no history to break. Files in the baseline that no longer exist are not reported: a line
    /// may be retired once its consumers are drained.
    /// </summary>
    /// <param name="baselineDirectory">The earlier schemas, such as a checkout of the base branch.</param>
    /// <param name="directory">The schemas as they stand now.</param>
    /// <exception cref="DirectoryNotFoundException">Either directory does not exist.</exception>
    public static ContractSchemaReport Compare(string baselineDirectory, string directory) =>
        new(CompareBaseline(SchemaFile.FindAll(directory), SchemaFile.FindAll(baselineDirectory)));

    /// <summary>
    /// Compares one earlier revision of a contract line with the current one.
    /// <para>
    /// Compatibility is not transitive: a revision can be compatible with the one before it and still break
    /// an older one, whose writers may still be running. A caller that can enumerate a line's history — from
    /// version control, or from an archive of what it published — should call this once per earlier revision
    /// rather than only against the newest. The <see cref="Compare(string, string)"/> overload compares
    /// against a single baseline, which is the common case in a pull request.
    /// </para>
    /// </summary>
    /// <param name="earlier">The earlier revision. Ignored when it declares a different <c>$id</c>.</param>
    /// <param name="current">The revision as it stands now.</param>
    /// <param name="earlierRevision">
    /// How to name <paramref name="earlier"/> in the findings, for example a commit hash.
    /// </param>
    public static ContractSchemaReport Compare(
        ContractSchemaDocument earlier,
        ContractSchemaDocument current,
        string earlierRevision = "the earlier revision")
    {
        ArgumentNullException.ThrowIfNull(earlier);
        ArgumentNullException.ThrowIfNull(current);

        var findings = new List<ContractSchemaFinding>();
        CompareRevision(current.Id, current, earlier, earlierRevision, findings);
        return new ContractSchemaReport(findings);
    }

    /// <summary>Compares each file with the file declaring the same $id in <paramref name="baseline"/>.</summary>
    private static List<ContractSchemaFinding> CompareBaseline(
        IReadOnlyList<SchemaFile> files,
        IReadOnlyList<SchemaFile> baseline)
    {
        var findings = new List<ContractSchemaFinding>();
        var earlierById = new Dictionary<string, SchemaFile>(StringComparer.Ordinal);
        foreach (var file in baseline)
        {
            if (SchemaFile.TryLoad(file.Json, file.RelativePath, []) is { } document)
                earlierById.TryAdd(document.Id, file);
        }

        foreach (var file in files)
        {
            var current = file.TryLoad(findings);
            if (current is null || !earlierById.TryGetValue(current.Id, out var earlier) || earlier.Json == file.Json)
                continue;

            // A revision that no longer loads isn't part of this line's history.
            if (SchemaFile.TryLoad(earlier.Json, file.RelativePath, []) is { } document)
            {
                CompareRevision(
                    file.RelativePath, current, document, $"baseline {earlier.RelativePath}", findings);
            }
        }

        return findings;
    }

    private static void CompareRevision(
        string file,
        ContractSchemaDocument current,
        ContractSchemaDocument earlier,
        string revision,
        List<ContractSchemaFinding> findings)
    {
        // A revision that declared another $id isn't part of this line's history.
        if (earlier.Id != current.Id)
            return;

        if (earlier.LegacyVersion != current.LegacyVersion)
        {
            findings.Add(new ContractSchemaFinding(file, "/x-runax-legacy-version", ContractSchemaSeverity.Error,
                $"the legacy version changed from {Show(earlier.LegacyVersion)} to {Show(current.LegacyVersion)} " +
                $"since {revision}. It must never change within a $id."));
        }

        foreach (var (pointer, severity, message) in CompatibilityChecker.Compare(earlier, current))
        {
            var advice = severity == ContractSchemaSeverity.Error ? $" {Advice(current.Id)}" : "";
            findings.Add(new ContractSchemaFinding(file, pointer, severity, $"{message} since {revision}.{advice}"));
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
