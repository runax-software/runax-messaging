using System.Text.Json;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// A loaded contract schema: one JSON Schema document (draft 2020-12, contract profile) identified by its
/// <c>$id</c>, compiled for validation.
/// </summary>
public sealed class ContractSchemaDocument
{
    private static readonly JsonElement EmptyObject = JsonDocument.Parse("{}").RootElement.Clone();

    private readonly SchemaNode _root;

    private ContractSchemaDocument(string id, int? legacyVersion, string json, SchemaNode root)
    {
        Id = id;
        LegacyVersion = legacyVersion;
        Json = json;
        _root = root;
        IsSelective = !SchemaEvaluator.IsValid(root, EmptyObject);
    }

    /// <summary>Gets the compiled root schema.</summary>
    internal SchemaNode Root => _root;

    /// <summary>Gets the document's <c>$id</c>, which identifies its contract line.</summary>
    public string Id { get; }

    /// <summary>
    /// Gets the integer contract version the line records in <c>x-runax-legacy-version</c>, if any. Consumers
    /// of the line accept messages carrying it, which keeps integer-only producers working.
    /// </summary>
    public int? LegacyVersion { get; }

    /// <summary>Gets the document's JSON text.</summary>
    public string Json { get; }

    /// <summary>
    /// Gets whether the line rejects at least the empty object. A line that accepts any JSON object can't
    /// tell messages apart, so it takes no part in routing messages that carry no contract identity.
    /// </summary>
    public bool IsSelective { get; }

    /// <summary>
    /// Parses and compiles a schema document.
    /// </summary>
    /// <param name="json">The document's JSON text.</param>
    /// <param name="source">Where the document came from, for error messages.</param>
    /// <returns>The loaded document.</returns>
    /// <exception cref="ContractSchemaException">The document can't be loaded.</exception>
    public static ContractSchemaDocument Parse(string json, string? source = null)
    {
        ArgumentNullException.ThrowIfNull(json);

        JsonElement root;
        try
        {
            using var document = JsonDocument.Parse(json);
            root = document.RootElement.Clone();
        }
        catch (JsonException ex)
        {
            throw new ContractSchemaException($"Contract schema {source ?? "document"} is not valid JSON: {ex.Message}", ex);
        }

        if (root.ValueKind != JsonValueKind.Object
            || !root.TryGetProperty("$id", out var idElement)
            || idElement.ValueKind != JsonValueKind.String
            || !Uri.TryCreate(idElement.GetString(), UriKind.Absolute, out _))
        {
            throw new ContractSchemaException(
                $"Contract schema {source ?? "document"} must be an object with an absolute URI '$id'.");
        }

        var id = ContractSchemaId.Normalize(idElement.GetString()!);

        int? legacyVersion = null;
        if (root.TryGetProperty("x-runax-legacy-version", out var legacy))
        {
            legacyVersion = legacy.ValueKind == JsonValueKind.Number && legacy.TryGetInt32(out var version)
                ? version
                : throw new ContractSchemaException($"Contract schema '{id}': 'x-runax-legacy-version' must be an integer.");
        }

        return new ContractSchemaDocument(id, legacyVersion, json, SchemaCompiler.Compile(root, id));
    }

    /// <summary>
    /// Validates a JSON value against the document.
    /// </summary>
    /// <param name="instance">The value to validate, without the <c>__runax</c> envelope key.</param>
    /// <returns>The outcome, with up to ten failures as <c>"&lt;json pointer&gt;: &lt;reason&gt;"</c>.</returns>
    public ContractValidationResult Validate(JsonElement instance)
    {
        var errors = new List<string>();
        return SchemaEvaluator.IsValid(_root, instance, errors)
            ? ContractValidationResult.Success
            : new ContractValidationResult(errors);
    }
}

/// <summary>The outcome of validating a value against a <see cref="ContractSchemaDocument"/>.</summary>
public sealed class ContractValidationResult
{
    internal ContractValidationResult(IReadOnlyList<string> errors) => Errors = errors;

    /// <summary>Gets a successful result.</summary>
    public static ContractValidationResult Success { get; } = new([]);

    /// <summary>Gets whether the value is valid.</summary>
    public bool IsValid => Errors.Count == 0;

    /// <summary>Gets the failures, each <c>"&lt;json pointer&gt;: &lt;reason&gt;"</c>.</summary>
    public IReadOnlyList<string> Errors { get; }
}
