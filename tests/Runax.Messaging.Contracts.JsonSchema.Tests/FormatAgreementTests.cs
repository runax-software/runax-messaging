using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Runax.Messaging.Contracts.JsonSchema;

namespace Runax.Messaging.Contracts.JsonSchema.Tests;

/// <summary>
/// The asserted formats must agree with System.Text.Json: whatever validation accepts deserializes, and
/// whatever it rejects doesn't.
/// </summary>
public class FormatAgreementTests
{
    public static TheoryData<string, string> Samples => new()
    {
        { "uuid", "\"8a6f3e2c-1d4b-4c7e-9f0a-2b3c4d5e6f70\"" },
        { "uuid", "\"8A6F3E2C-1D4B-4C7E-9F0A-2B3C4D5E6F70\"" },
        { "uuid", "\"not-a-guid\"" },
        { "uuid", "\"8a6f3e2c1d4b4c7e9f0a2b3c4d5e6f70\"" },
        { "date-time", "\"2026-10-20T12:30:00Z\"" },
        { "date-time", "\"2026-10-20T12:30:00+02:00\"" },
        { "date-time", "\"2026-10-20T12:30:00\"" },
        { "date-time", "\"2026-10-20\"" },
        { "date-time", "\"yesterday\"" },
        { "date", "\"2026-10-20\"" },
        { "date", "\"2026-13-01\"" },
        { "date", "\"20/10/2026\"" },
        { "time", "\"12:30:00\"" },
        { "time", "\"12:30:00.1234567\"" },
        { "time", "\"25:00:00\"" },
        { "time", "\"noon\"" },
    };

    [Theory]
    [MemberData(nameof(Samples))]
    public void Validation_agrees_with_deserialization(string format, string json)
    {
        using var parsed = JsonDocument.Parse(json);
        var value = parsed.RootElement;

        var validates = SchemaEvaluator.MatchesFormat(format, value);
        var deserializes = format switch
        {
            "uuid" => Deserializes(value, SampleJsonContext.Default.Guid),
            "date-time" => Deserializes(value, SampleJsonContext.Default.DateTimeOffset)
                           || Deserializes(value, SampleJsonContext.Default.DateTime),
            "date" => Deserializes(value, SampleJsonContext.Default.DateOnly),
            "time" => Deserializes(value, SampleJsonContext.Default.TimeOnly),
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };

        validates.ShouldBe(deserializes, $"{format} {json}");
    }

    [Fact]
    public void Other_formats_are_annotations()
    {
        using var parsed = JsonDocument.Parse("\"not an email\"");

        SchemaEvaluator.MatchesFormat("email", parsed.RootElement).ShouldBeTrue();
    }

    private static bool Deserializes<T>(JsonElement value, JsonTypeInfo<T> typeInfo)
    {
        try
        {
            value.Deserialize(typeInfo);
            return true;
        }
        catch (Exception ex) when (ex is JsonException or FormatException)
        {
            return false;
        }
    }
}

[JsonSerializable(typeof(Guid))]
[JsonSerializable(typeof(DateTime))]
[JsonSerializable(typeof(DateTimeOffset))]
[JsonSerializable(typeof(DateOnly))]
[JsonSerializable(typeof(TimeOnly))]
internal sealed partial class SampleJsonContext : JsonSerializerContext;
