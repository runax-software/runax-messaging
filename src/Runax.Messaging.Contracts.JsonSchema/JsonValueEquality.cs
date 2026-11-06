using System.Text.Json;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Structural equality for JSON values, as JSON Schema defines it: objects are equal when they carry the
/// same property names with equal values, in any order; arrays when they carry equal values in the same
/// order; and numbers when their mathematical values are equal, so <c>1</c> equals <c>1.0</c>.
/// <para>
/// This stands in for <see cref="JsonElement"/>'s own <c>DeepEquals</c>, which only ships in .NET 9 and
/// later. It is used on every target framework rather than on net8.0 alone deliberately: <c>const</c>,
/// <c>enum</c> and <c>uniqueItems</c> decide whether a message is valid, and a contract that validated
/// differently depending on the runtime underneath would defeat the point of having a contract.
/// </para>
/// </summary>
internal static class JsonValueEquality
{
    public static bool Equal(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
            return false;

        return a.ValueKind switch
        {
            JsonValueKind.Null or JsonValueKind.True or JsonValueKind.False => true,
            JsonValueKind.String => string.Equals(a.GetString(), b.GetString(), StringComparison.Ordinal),
            JsonValueKind.Number => NumbersEqual(a, b),
            JsonValueKind.Array => ArraysEqual(a, b),
            JsonValueKind.Object => ObjectsEqual(a, b),
            _ => false,
        };
    }

    private static bool NumbersEqual(JsonElement a, JsonElement b)
    {
        // Identical text is the common case by far, and needs no parsing.
        if (string.Equals(a.GetRawText(), b.GetRawText(), StringComparison.Ordinal))
            return true;

        // decimal holds every digit of the numbers a contract schema realistically carries, and compares
        // by value rather than by scale, so 1.0 == 1.
        if (a.TryGetDecimal(out var x) && b.TryGetDecimal(out var y))
            return x == y;

        // Outside decimal's range, compare as doubles — but only where both are finite. Numbers past
        // double's range parse to infinity, which would make every one of them equal to every other.
        if (a.TryGetDouble(out var p) && b.TryGetDouble(out var q) && double.IsFinite(p) && double.IsFinite(q))
            return p == q;

        // Not comparable as a number, and the raw text already differs. Calling that unequal can only
        // reject a value that two unequal spellings of an unrepresentable number would have accepted.
        return false;
    }

    private static bool ArraysEqual(JsonElement a, JsonElement b)
    {
        if (a.GetArrayLength() != b.GetArrayLength())
            return false;

        var other = b.EnumerateArray();
        foreach (var item in a.EnumerateArray())
        {
            other.MoveNext();
            if (!Equal(item, other.Current))
                return false;
        }

        return true;
    }

    private static bool ObjectsEqual(JsonElement a, JsonElement b)
    {
        if (a.EnumerateObject().Count() != b.EnumerateObject().Count())
            return false;

        foreach (var property in a.EnumerateObject())
        {
            if (!b.TryGetProperty(property.Name, out var value) || !Equal(property.Value, value))
                return false;
        }

        return true;
    }
}
