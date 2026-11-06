namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>How serious a <see cref="ContractSchemaFinding"/> is.</summary>
public enum ContractSchemaSeverity
{
    /// <summary>Worth knowing, but the schema set is usable as it stands.</summary>
    Warning,

    /// <summary>The schema set is wrong, and a check over it should fail.</summary>
    Error,
}

/// <summary>One problem found in a contract schema file.</summary>
/// <param name="File">The file, relative to the schema directory it was found under.</param>
/// <param name="JsonPointer">Where in the schema the problem is; empty for the whole file.</param>
/// <param name="Severity">Whether the schema set is wrong, or only worth a note.</param>
/// <param name="Message">What's wrong, and when it helps, what to do about it.</param>
public sealed record ContractSchemaFinding(
    string File,
    string JsonPointer,
    ContractSchemaSeverity Severity,
    string Message)
{
    /// <summary>Renders the finding as <c>"error: &lt;file&gt; at &lt;pointer&gt;: &lt;message&gt;"</c>.</summary>
    public override string ToString() =>
        $"{(Severity == ContractSchemaSeverity.Error ? "error" : "warning")}: {File}" +
        $"{(JsonPointer.Length > 0 ? $" at {JsonPointer}" : "")}: {Message}";
}
