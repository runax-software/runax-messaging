using System.Collections;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// What a pass over a set of contract schema files found. Enumerating the report yields every finding;
/// <see cref="Errors"/> is what a test or a CI check should assert is empty.
/// </summary>
public sealed class ContractSchemaReport : IReadOnlyList<ContractSchemaFinding>
{
    private readonly IReadOnlyList<ContractSchemaFinding> _findings;

    internal ContractSchemaReport(IReadOnlyList<ContractSchemaFinding> findings) => _findings = findings;

    /// <summary>Gets the findings that make the schema set wrong.</summary>
    public IReadOnlyList<ContractSchemaFinding> Errors =>
        [.. _findings.Where(f => f.Severity == ContractSchemaSeverity.Error)];

    /// <summary>Gets the findings worth knowing about that don't make the schema set wrong.</summary>
    public IReadOnlyList<ContractSchemaFinding> Warnings =>
        [.. _findings.Where(f => f.Severity == ContractSchemaSeverity.Warning)];

    /// <summary>Gets a value indicating whether anything in the set is wrong.</summary>
    public bool HasErrors => _findings.Any(f => f.Severity == ContractSchemaSeverity.Error);

    /// <inheritdoc />
    public int Count => _findings.Count;

    /// <inheritdoc />
    public ContractSchemaFinding this[int index] => _findings[index];

    /// <inheritdoc />
    public IEnumerator<ContractSchemaFinding> GetEnumerator() => _findings.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>Renders one finding per line, so an assertion failure says what was wrong.</summary>
    public override string ToString() =>
        _findings.Count == 0 ? "no findings" : string.Join(Environment.NewLine, _findings);
}
