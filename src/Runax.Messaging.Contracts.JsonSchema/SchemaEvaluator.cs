using System.Text.Json;
using System.Text.Json.Serialization;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Validates a JSON instance against compiled <see cref="SchemaNode"/>s, collecting failures as
/// <c>"&lt;json pointer&gt;: &lt;reason&gt;"</c>. Formats in the asserted set are checked with
/// System.Text.Json's own converters, so validation agrees with deserialization; other formats are annotations.
/// </summary>
internal static class SchemaEvaluator
{
    /// <summary>Stop collecting after this many failures; one is enough to reject.</summary>
    private const int MaxErrors = 10;

    public static bool IsValid(SchemaNode schema, JsonElement instance, List<string>? errors = null) =>
        Evaluate(schema, instance, "", errors);

    private static bool Evaluate(SchemaNode node, JsonElement value, string pointer, List<string>? errors)
    {
        if (node.Rejects)
            return Fail(errors, pointer, "no value is allowed here");

        var valid = true;

        if (node.Ref is not null)
            valid &= Evaluate(node.Ref, value, pointer, errors);

        if (node.Types != JsonTypes.None && !MatchesType(node.Types, value))
            return Fail(errors, pointer, $"expected {Describe(node.Types)}, got {Describe(value)}");

        if (node.Const is { } constant && !JsonElement.DeepEquals(constant, value))
            valid &= Fail(errors, pointer, $"must be {constant.GetRawText()}");

        if (node.Enum is { } options && !options.Any(option => JsonElement.DeepEquals(option, value)))
            valid &= Fail(errors, pointer, "must be one of the enumerated values");

        switch (value.ValueKind)
        {
            case JsonValueKind.Object:
                valid &= EvaluateObject(node, value, pointer, errors);
                break;
            case JsonValueKind.Array:
                valid &= EvaluateArray(node, value, pointer, errors);
                break;
            case JsonValueKind.String:
                valid &= EvaluateString(node, value, pointer, errors);
                break;
            case JsonValueKind.Number:
                valid &= EvaluateNumber(node, value, pointer, errors);
                break;
        }

        if (node.AnyOf is { } anyOf && !anyOf.Any(branch => Evaluate(branch, value, pointer, null)))
            valid &= Fail(errors, pointer, "matches none of the allowed shapes (anyOf)");

        if (node.OneOf is { } oneOf)
        {
            var matches = oneOf.Count(branch => Evaluate(branch, value, pointer, null));
            if (matches != 1)
            {
                valid &= Fail(errors, pointer, matches == 0
                    ? "matches none of the allowed shapes (oneOf)"
                    : "matches more than one of the allowed shapes (oneOf)");
            }
        }

        return valid;
    }

    private static bool EvaluateObject(SchemaNode node, JsonElement value, string pointer, List<string>? errors)
    {
        var valid = true;
        var count = 0;

        foreach (var property in value.EnumerateObject())
        {
            count++;
            var at = $"{pointer}/{Escape(property.Name)}";

            if (node.Properties is not null && node.Properties.TryGetValue(property.Name, out var schema))
                valid &= Evaluate(schema, property.Value, at, errors);
            else if (node.AdditionalProperties is { } additional)
                valid &= additional.Rejects ? Fail(errors, at, "is not allowed") : Evaluate(additional, property.Value, at, errors);
        }

        if (node.Required is { } required)
        {
            foreach (var name in required)
            {
                if (!value.TryGetProperty(name, out _))
                    valid &= Fail(errors, $"{pointer}/{Escape(name)}", "is required");
            }
        }

        if (node.MinProperties is { } min && count < min)
            valid &= Fail(errors, pointer, $"must have at least {min} properties");
        if (node.MaxProperties is { } max && count > max)
            valid &= Fail(errors, pointer, $"must have at most {max} properties");

        return valid;
    }

    private static bool EvaluateArray(SchemaNode node, JsonElement value, string pointer, List<string>? errors)
    {
        var valid = true;
        var length = value.GetArrayLength();

        if (node.Items is { } items)
        {
            var index = 0;
            foreach (var item in value.EnumerateArray())
                valid &= Evaluate(items, item, $"{pointer}/{index++}", errors);
        }

        if (node.MinItems is { } min && length < min)
            valid &= Fail(errors, pointer, $"must have at least {min} items");
        if (node.MaxItems is { } max && length > max)
            valid &= Fail(errors, pointer, $"must have at most {max} items");

        if (node.UniqueItems)
        {
            var elements = value.EnumerateArray().ToArray();
            for (var i = 0; i < elements.Length; i++)
            {
                for (var j = i + 1; j < elements.Length; j++)
                {
                    if (JsonElement.DeepEquals(elements[i], elements[j]))
                        return Fail(errors, pointer, $"items {i} and {j} are equal, but items must be unique");
                }
            }
        }

        return valid;
    }

    private static bool EvaluateString(SchemaNode node, JsonElement value, string pointer, List<string>? errors)
    {
        var valid = true;
        var text = value.GetString()!;

        if (node.MinLength is not null || node.MaxLength is not null)
        {
            // JSON Schema counts code points, not UTF-16 units.
            var length = text.EnumerateRunes().Count();
            if (node.MinLength is { } min && length < min)
                valid &= Fail(errors, pointer, $"must be at least {min} characters");
            if (node.MaxLength is { } max && length > max)
                valid &= Fail(errors, pointer, $"must be at most {max} characters");
        }

        if (node.Pattern is { } pattern && !pattern.IsMatch(text))
            valid &= Fail(errors, pointer, $"must match the pattern '{pattern}'");

        if (node.Format is { } format && !MatchesFormat(format, value))
            valid &= Fail(errors, pointer, $"is not a valid {format}");

        return valid;
    }

    private static bool EvaluateNumber(SchemaNode node, JsonElement value, string pointer, List<string>? errors)
    {
        if (node.Minimum is null && node.Maximum is null && node.ExclusiveMinimum is null
            && node.ExclusiveMaximum is null && node.MultipleOf is null)
        {
            return true;
        }

        if (!value.TryGetDecimal(out var number))
            return Fail(errors, pointer, "is outside the range this schema's bounds can check");

        var valid = true;
        if (node.Minimum is { } minimum && number < minimum)
            valid &= Fail(errors, pointer, $"must be at least {minimum}");
        if (node.Maximum is { } maximum && number > maximum)
            valid &= Fail(errors, pointer, $"must be at most {maximum}");
        if (node.ExclusiveMinimum is { } exclusiveMinimum && number <= exclusiveMinimum)
            valid &= Fail(errors, pointer, $"must be greater than {exclusiveMinimum}");
        if (node.ExclusiveMaximum is { } exclusiveMaximum && number >= exclusiveMaximum)
            valid &= Fail(errors, pointer, $"must be less than {exclusiveMaximum}");
        if (node.MultipleOf is { } factor && number % factor != 0)
            valid &= Fail(errors, pointer, $"must be a multiple of {factor}");

        return valid;
    }

    private static bool MatchesType(JsonTypes types, JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null => types.HasFlag(JsonTypes.Null),
        JsonValueKind.True or JsonValueKind.False => types.HasFlag(JsonTypes.Boolean),
        JsonValueKind.Object => types.HasFlag(JsonTypes.Object),
        JsonValueKind.Array => types.HasFlag(JsonTypes.Array),
        JsonValueKind.String => types.HasFlag(JsonTypes.String),
        JsonValueKind.Number => types.HasFlag(JsonTypes.Number) || (types.HasFlag(JsonTypes.Integer) && IsInteger(value)),
        _ => false,
    };

    // A JSON Schema integer is any number without a fractional part, including 1.0.
    private static bool IsInteger(JsonElement value) =>
        value.TryGetDecimal(out var number)
            ? number == decimal.Truncate(number)
            : value.TryGetDouble(out var big) && double.IsFinite(big) && Math.Floor(big) == big;

    /// <summary>
    /// Asserts the formats the exporter emits for types whose converters reject bad input, using those
    /// converters. Every other format is an annotation.
    /// </summary>
    internal static bool MatchesFormat(string format, JsonElement value) => format switch
    {
        "uuid" => value.TryGetGuid(out _),
        "date-time" => value.TryGetDateTimeOffset(out _) || value.TryGetDateTime(out _),
        "date" => Converts(value, FormatJsonContext.Default.DateOnly),
        "time" => Converts(value, FormatJsonContext.Default.TimeOnly),
        _ => true,
    };

    private static bool Converts<T>(JsonElement value, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> typeInfo)
    {
        try
        {
            value.Deserialize(typeInfo);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static bool Fail(List<string>? errors, string pointer, string reason)
    {
        if (errors is not null && errors.Count < MaxErrors)
            errors.Add($"{(pointer.Length == 0 ? "/" : pointer)}: {reason}");
        return false;
    }

    private static string Describe(JsonTypes types) =>
        string.Join(" or ", Enum.GetValues<JsonTypes>()
            .Where(t => t != JsonTypes.None && types.HasFlag(t))
            .Select(t => t.ToString().ToLowerInvariant()));

    private static string Describe(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.True or JsonValueKind.False => "boolean",
        JsonValueKind.Number => IsInteger(value) ? "integer" : "number",
        var kind => kind.ToString().ToLowerInvariant(),
    };

    private static string Escape(string name) =>
        name.Replace("~", "~0", StringComparison.Ordinal).Replace("/", "~1", StringComparison.Ordinal);
}

/// <summary>Source-generated metadata for the System.Text.Json converters that assert formats.</summary>
[JsonSerializable(typeof(DateOnly))]
[JsonSerializable(typeof(TimeOnly))]
internal sealed partial class FormatJsonContext : JsonSerializerContext;
