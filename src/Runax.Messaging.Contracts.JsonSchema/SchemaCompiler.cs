using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Compiles a schema document into <see cref="SchemaNode"/>s. Only the contract profile is accepted:
/// keywords the profile excludes make loading fail, because validating with a keyword silently ignored is
/// worse than not validating. Annotations (<c>title</c>, <c>description</c>, <c>default</c>, <c>x-*</c>, ...)
/// are skipped.
/// </summary>
internal sealed class SchemaCompiler
{
    /// <summary>Keywords the profile excludes. Loading a document that uses one fails.</summary>
    internal static readonly HashSet<string> Unsupported = new(StringComparer.Ordinal)
    {
        "if", "then", "else", "not", "allOf", "patternProperties", "dependentSchemas", "dependentRequired",
        "unevaluatedProperties", "unevaluatedItems", "prefixItems", "contains", "minContains", "maxContains",
        "propertyNames", "$dynamicRef", "$dynamicAnchor", "$recursiveRef", "$recursiveAnchor", "$anchor",
        "dependencies", "additionalItems",
    };

    /// <summary>Keywords that don't affect validation (besides <c>x-*</c> ones).</summary>
    internal static readonly HashSet<string> Annotations = new(StringComparer.Ordinal)
    {
        "$schema", "$id", "$comment", "$defs", "definitions", "title", "description", "default", "examples",
        "deprecated", "readOnly", "writeOnly", "contentMediaType", "contentEncoding",
    };

    /// <summary>Keywords the evaluator checks.</summary>
    internal static readonly HashSet<string> Evaluated = new(StringComparer.Ordinal)
    {
        "type", "properties", "required", "additionalProperties", "minProperties", "maxProperties", "items",
        "minItems", "maxItems", "uniqueItems", "enum", "const", "format", "anyOf", "oneOf", "$ref", "minimum",
        "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "minLength", "maxLength", "pattern",
    };

    /// <summary>The formats validation asserts; every other format is an annotation.</summary>
    internal static readonly HashSet<string> AssertedFormats = new(StringComparer.Ordinal)
    {
        "uuid", "date-time", "date", "time",
    };

    private readonly JsonElement _root;
    private readonly string _documentId;
    private readonly Dictionary<string, SchemaNode> _byPointer = new(StringComparer.Ordinal);

    private SchemaCompiler(JsonElement root, string documentId)
    {
        _root = root;
        _documentId = documentId;
    }

    /// <summary>Compiles <paramref name="root"/>, the whole document.</summary>
    /// <exception cref="ContractSchemaException">The document uses something outside the contract profile.</exception>
    public static SchemaNode Compile(JsonElement root, string documentId)
    {
        var compiler = new SchemaCompiler(root, documentId);
        return compiler.CompileAt(root, "#");
    }

    private SchemaNode CompileAt(JsonElement schema, string pointer)
    {
        if (_byPointer.TryGetValue(pointer, out var existing))
            return existing;

        switch (schema.ValueKind)
        {
            case JsonValueKind.True:
                return SchemaNode.AlwaysValid;
            case JsonValueKind.False:
                return SchemaNode.NeverValid;
            case JsonValueKind.Object:
                break;
            default:
                throw Error(pointer, "a schema must be an object or a boolean");
        }

        // Registered before its children compile, so a recursive $ref resolves to this same node.
        var node = new SchemaNode();
        _byPointer[pointer] = node;

        foreach (var keyword in schema.EnumerateObject())
        {
            var name = keyword.Name;
            var value = keyword.Value;
            var at = $"{pointer}/{Escape(name)}";

            if (Unsupported.Contains(name))
                throw Error(pointer, $"'{name}' is not part of the contract profile");

            if (name == "$id" && pointer != "#")
                throw Error(pointer, "nested '$id' is not part of the contract profile");

            if (name == "default")
            {
                node.Default = value.Clone();
                continue;
            }

            if (Annotations.Contains(name) || name.StartsWith("x-", StringComparison.Ordinal))
                continue;

            switch (name)
            {
                case "type":
                    node.Types = ReadTypes(value, at);
                    break;
                case "properties":
                    node.Properties = new Dictionary<string, SchemaNode>(StringComparer.Ordinal);
                    foreach (var property in RequireObject(value, at).EnumerateObject())
                        node.Properties[property.Name] = CompileAt(property.Value, $"{at}/{Escape(property.Name)}");
                    break;
                case "required":
                    node.Required = ReadStrings(value, at);
                    break;
                case "additionalProperties":
                    node.AdditionalProperties = CompileAt(value, at);
                    break;
                case "minProperties":
                    node.MinProperties = ReadCount(value, at);
                    break;
                case "maxProperties":
                    node.MaxProperties = ReadCount(value, at);
                    break;
                case "items":
                    node.Items = CompileAt(value, at);
                    break;
                case "minItems":
                    node.MinItems = ReadCount(value, at);
                    break;
                case "maxItems":
                    node.MaxItems = ReadCount(value, at);
                    break;
                case "uniqueItems":
                    node.UniqueItems = value.ValueKind == JsonValueKind.True;
                    break;
                case "enum":
                    node.Enum = [.. RequireArray(value, at).EnumerateArray().Select(e => e.Clone())];
                    break;
                case "const":
                    node.Const = value.Clone();
                    break;
                case "format":
                    node.Format = value.ValueKind == JsonValueKind.String
                        ? value.GetString()
                        : throw Error(at, "must be a string");
                    break;
                case "anyOf":
                    node.AnyOf = CompileAll(value, at);
                    break;
                case "oneOf":
                    node.OneOf = CompileAll(value, at);
                    break;
                case "$ref":
                    node.Ref = ResolveRef(value, at);
                    break;
                case "minimum":
                    node.Minimum = ReadNumber(value, at);
                    break;
                case "maximum":
                    node.Maximum = ReadNumber(value, at);
                    break;
                case "exclusiveMinimum":
                    node.ExclusiveMinimum = ReadNumber(value, at);
                    break;
                case "exclusiveMaximum":
                    node.ExclusiveMaximum = ReadNumber(value, at);
                    break;
                case "multipleOf":
                    node.MultipleOf = ReadNumber(value, at) is > 0 and var factor
                        ? factor
                        : throw Error(at, "must be greater than 0");
                    break;
                case "minLength":
                    node.MinLength = ReadCount(value, at);
                    break;
                case "maxLength":
                    node.MaxLength = ReadCount(value, at);
                    break;
                case "pattern":
                    node.Pattern = ReadPattern(value, at);
                    break;
                default:
                    // Unknown keywords are annotations in JSON Schema; the linter is where they're flagged.
                    break;
            }
        }

        return node;
    }

    private SchemaNode[] CompileAll(JsonElement value, string at)
    {
        var branches = RequireArray(value, at).EnumerateArray().ToArray();
        if (branches.Length == 0)
            throw Error(at, "must not be empty");

        return [.. branches.Select((branch, i) => CompileAt(branch, $"{at}/{i}"))];
    }

    // Only local references (a JSON pointer into this document) are in the profile.
    private SchemaNode ResolveRef(JsonElement value, string at)
    {
        var reference = value.ValueKind == JsonValueKind.String
            ? value.GetString()!
            : throw Error(at, "must be a string");

        if (reference != "#" && !reference.StartsWith("#/", StringComparison.Ordinal))
            throw Error(at, $"only local references are part of the contract profile, not '{reference}'");

        var target = _root;
        foreach (var segment in reference.Length <= 2 ? [] : reference[2..].Split('/'))
        {
            var token = Uri.UnescapeDataString(segment).Replace("~1", "/", StringComparison.Ordinal)
                .Replace("~0", "~", StringComparison.Ordinal);

            if (target.ValueKind == JsonValueKind.Object && target.TryGetProperty(token, out var child))
                target = child;
            else if (target.ValueKind == JsonValueKind.Array
                     && int.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out var index)
                     && index < target.GetArrayLength())
                target = target[index];
            else
                throw Error(at, $"reference '{reference}' does not resolve");
        }

        return CompileAt(target, reference);
    }

    private JsonTypes ReadTypes(JsonElement value, string at)
    {
        var names = value.ValueKind == JsonValueKind.String ? [value.GetString()!] : ReadStrings(value, at);
        var types = JsonTypes.None;
        foreach (var name in names)
        {
            types |= name switch
            {
                "null" => JsonTypes.Null,
                "boolean" => JsonTypes.Boolean,
                "object" => JsonTypes.Object,
                "array" => JsonTypes.Array,
                "number" => JsonTypes.Number,
                "integer" => JsonTypes.Integer,
                "string" => JsonTypes.String,
                _ => throw Error(at, $"unknown type '{name}'"),
            };
        }

        return types;
    }

    private string[] ReadStrings(JsonElement value, string at) =>
        [.. RequireArray(value, at).EnumerateArray().Select(e => e.ValueKind == JsonValueKind.String
            ? e.GetString()!
            : throw Error(at, "must be an array of strings"))];

    private int ReadCount(JsonElement value, string at) =>
        value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var count) && count >= 0
            ? count
            : throw Error(at, "must be a non-negative integer");

    private decimal ReadNumber(JsonElement value, string at) =>
        value.ValueKind == JsonValueKind.Number && value.TryGetDecimal(out var number)
            ? number
            : throw Error(at, "must be a number");

    private Regex ReadPattern(JsonElement value, string at)
    {
        var pattern = value.ValueKind == JsonValueKind.String ? value.GetString()! : throw Error(at, "must be a string");
        try
        {
            return new Regex(pattern, RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
        }
        catch (ArgumentException ex)
        {
            throw Error(at, $"invalid pattern: {ex.Message}");
        }
    }

    private JsonElement RequireObject(JsonElement value, string at) =>
        value.ValueKind == JsonValueKind.Object ? value : throw Error(at, "must be an object");

    private JsonElement RequireArray(JsonElement value, string at) =>
        value.ValueKind == JsonValueKind.Array ? value : throw Error(at, "must be an array");

    private ContractSchemaException Error(string pointer, string message) =>
        new($"Contract schema '{_documentId}' at {pointer}: {message}.");

    private static string Escape(string name) =>
        name.Replace("~", "~0", StringComparison.Ordinal).Replace("/", "~1", StringComparison.Ordinal);
}
