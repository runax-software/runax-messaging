using System.Text.Json;

namespace Runax.Messaging.Contracts.JsonSchema.Tests;

/// <summary>
/// The stand-in for <c>JsonElement.DeepEquals</c>, which only ships in .NET 9. It is used on every target
/// framework, so <c>const</c>, <c>enum</c> and <c>uniqueItems</c> decide the same way on all of them.
/// </summary>
public class JsonValueEqualityTests
{
    public static TheoryData<string, string, bool> Pairs => new()
    {
        // Scalars.
        { "null", "null", true },
        { "true", "true", true },
        { "true", "false", false },
        { "\"a\"", "\"a\"", true },
        { "\"a\"", "\"b\"", false },
        { "\"1\"", "1", false },
        { "null", "0", false },
        { "null", "false", false },

        // Numbers compare by mathematical value, as JSON Schema requires.
        { "1", "1", true },
        { "1", "1.0", true },
        { "1", "1.000", true },
        { "100", "1e2", true },
        { "-0", "0", true },
        { "0.1", "0.2", false },
        { "1", "2", false },
        // Beyond decimal's range, so the double fallback decides.
        { "1e400", "1e400", true },
        { "1e400", "1e401", false },
        // 29 significant digits: past decimal, within double.
        { "1.2345678901234567890123456789", "1.2345678901234567890123456789", true },

        // Arrays are ordered.
        { "[]", "[]", true },
        { "[1,2]", "[1,2]", true },
        { "[1,2]", "[2,1]", false },
        { "[1,2]", "[1]", false },
        { "[1,2]", "[1.0,2.0]", true },
        { "[{\"a\":1}]", "[{\"a\":1.0}]", true },

        // Objects are not, but every property has to be present on both sides.
        { "{}", "{}", true },
        { "{\"a\":1,\"b\":2}", "{\"b\":2,\"a\":1}", true },
        { "{\"a\":1}", "{\"a\":1,\"b\":2}", false },
        { "{\"a\":1,\"b\":2}", "{\"a\":1}", false },
        { "{\"a\":1}", "{\"b\":1}", false },
        { "{\"a\":{\"b\":[1,{\"c\":null}]}}", "{\"a\":{\"b\":[1,{\"c\":null}]}}", true },
        { "{\"a\":{\"b\":[1,{\"c\":null}]}}", "{\"a\":{\"b\":[1,{\"c\":0}]}}", false },
    };

    [Theory]
    [MemberData(nameof(Pairs))]
    public void Values_are_equal_when_their_JSON_is_equal(string left, string right, bool expected)
    {
        using var a = JsonDocument.Parse(left);
        using var b = JsonDocument.Parse(right);

        JsonValueEquality.Equal(a.RootElement, b.RootElement).ShouldBe(expected);
        JsonValueEquality.Equal(b.RootElement, a.RootElement).ShouldBe(expected, "equality must be symmetric");
    }
}
