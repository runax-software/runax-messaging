using System.Text.Json;
using System.Text.RegularExpressions;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// The JSON types a schema's <c>type</c> keyword can name. <see cref="Integer"/> is a subset of
/// <see cref="Number"/>: a number with no fractional part.
/// </summary>
[Flags]
internal enum JsonTypes
{
    None = 0,
    Null = 1,
    Boolean = 2,
    Object = 4,
    Array = 8,
    Number = 16,
    Integer = 32,
    String = 64,
}

/// <summary>
/// One compiled (sub)schema of the contract profile. A <see langword="null"/> keyword means the schema doesn't
/// constrain it. Boolean schemas compile to <see cref="AlwaysValid"/> or <see cref="NeverValid"/>.
/// </summary>
internal sealed class SchemaNode
{
    public static SchemaNode AlwaysValid { get; } = new();

    public static SchemaNode NeverValid { get; } = new() { Rejects = true };

    /// <summary>Gets whether this is the boolean schema <c>false</c>.</summary>
    public bool Rejects { get; init; }

    public JsonTypes Types { get; set; }

    public Dictionary<string, SchemaNode>? Properties { get; set; }

    public string[]? Required { get; set; }

    public SchemaNode? AdditionalProperties { get; set; }

    public int? MinProperties { get; set; }

    public int? MaxProperties { get; set; }

    public SchemaNode? Items { get; set; }

    public int? MinItems { get; set; }

    public int? MaxItems { get; set; }

    public bool UniqueItems { get; set; }

    public JsonElement[]? Enum { get; set; }

    public JsonElement? Const { get; set; }

    public string? Format { get; set; }

    public SchemaNode[]? AnyOf { get; set; }

    public SchemaNode[]? OneOf { get; set; }

    /// <summary>Gets or sets the resolved target of a local <c>$ref</c>, evaluated alongside sibling keywords.</summary>
    public SchemaNode? Ref { get; set; }

    public decimal? Minimum { get; set; }

    public decimal? Maximum { get; set; }

    public decimal? ExclusiveMinimum { get; set; }

    public decimal? ExclusiveMaximum { get; set; }

    public decimal? MultipleOf { get; set; }

    public int? MinLength { get; set; }

    public int? MaxLength { get; set; }

    public Regex? Pattern { get; set; }

    /// <summary>Gets or sets the <c>default</c> annotation. Not used for validation, only for comparing revisions.</summary>
    public JsonElement? Default { get; set; }
}
