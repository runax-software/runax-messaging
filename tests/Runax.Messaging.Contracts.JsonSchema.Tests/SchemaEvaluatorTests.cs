using System.Text.Json;
using Runax.Messaging.Contracts.JsonSchema;

namespace Runax.Messaging.Contracts.JsonSchema.Tests;

public class SchemaEvaluatorTests
{
    [Theory]
    [InlineData("""{"type":"integer"}""", "1", true)]
    [InlineData("""{"type":"integer"}""", "1.0", true)]
    [InlineData("""{"type":"integer"}""", "1.5", false)]
    [InlineData("""{"type":"number"}""", "1.5", true)]
    [InlineData("""{"type":["string","null"]}""", "null", true)]
    [InlineData("""{"type":"string"}""", "null", false)]
    [InlineData("""{"type":"boolean"}""", "true", true)]
    [InlineData("""{"const":"order.placed"}""", "\"order.placed\"", true)]
    [InlineData("""{"const":"order.placed"}""", "\"order.cancelled\"", false)]
    [InlineData("""{"enum":["a","b"]}""", "\"b\"", true)]
    [InlineData("""{"enum":["a","b"]}""", "\"c\"", false)]
    [InlineData("""{"minimum":1,"maximum":3}""", "3", true)]
    [InlineData("""{"minimum":1,"maximum":3}""", "4", false)]
    [InlineData("""{"exclusiveMinimum":1}""", "1", false)]
    [InlineData("""{"multipleOf":0.5}""", "2.5", true)]
    [InlineData("""{"multipleOf":0.5}""", "2.25", false)]
    [InlineData("""{"minLength":2,"maxLength":2}""", "\"😀😀\"", true)]
    [InlineData("""{"maxLength":1}""", "\"😀😀\"", false)]
    [InlineData("""{"pattern":"^[A-Z]{3}$"}""", "\"USD\"", true)]
    [InlineData("""{"pattern":"^[A-Z]{3}$"}""", "\"usd\"", false)]
    [InlineData("""{"items":{"type":"integer"},"minItems":1}""", "[1,2]", true)]
    [InlineData("""{"items":{"type":"integer"}}""", "[1,\"x\"]", false)]
    [InlineData("""{"minItems":1}""", "[]", false)]
    [InlineData("""{"uniqueItems":true}""", "[1,2,1]", false)]
    [InlineData("""{"anyOf":[{"type":"string"},{"type":"integer"}]}""", "5", true)]
    [InlineData("""{"anyOf":[{"type":"string"},{"type":"integer"}]}""", "true", false)]
    [InlineData("""{"oneOf":[{"type":"number"},{"type":"integer"}]}""", "5", false)]
    [InlineData("""{"oneOf":[{"type":"number"},{"type":"integer"}]}""", "5.5", true)]
    [InlineData("""{"required":["a"],"properties":{"a":{"type":"string"}}}""", """{"a":"x","extra":1}""", true)]
    [InlineData("""{"required":["a"]}""", """{"b":1}""", false)]
    [InlineData("""{"properties":{"a":{"type":"string"}},"additionalProperties":false}""", """{"a":"x","b":1}""", false)]
    [InlineData("""{"additionalProperties":{"type":"integer"}}""", """{"a":1,"b":2}""", true)]
    [InlineData("""{"minProperties":1}""", "{}", false)]
    public void Keywords_of_the_contract_profile_are_evaluated(string schema, string instance, bool valid)
    {
        Validate(schema, instance).IsValid.ShouldBe(valid);
    }

    [Fact]
    public void Local_references_resolve_including_recursive_ones()
    {
        const string schema = """
            {
              "$defs": { "node": { "type": "object", "properties": { "next": { "$ref": "#/$defs/node" }, "value": { "type": "integer" } }, "required": ["value"] } },
              "$ref": "#/$defs/node"
            }
            """;

        Validate(schema, """{"value":1,"next":{"value":2,"next":{"value":3}}}""").IsValid.ShouldBeTrue();
        Validate(schema, """{"value":1,"next":{"next":{"value":3}}}""").IsValid.ShouldBeFalse();
    }

    [Fact]
    public void Failures_carry_json_pointers()
    {
        var result = Validate(
            """{"properties":{"lines":{"items":{"required":["sku"]}}}}""",
            """{"lines":[{"sku":"a"},{"qty":1}]}""");

        result.Errors.ShouldBe(["/lines/1/sku: is required"]);
    }

    [Fact]
    public void A_type_mismatch_at_the_root_is_reported_at_slash()
    {
        Validate("""{"type":"object"}""", "[]").Errors.ShouldBe(["/: expected object, got array"]);
    }

    private static ContractValidationResult Validate(string schema, string instance)
    {
        var document = ContractSchemaDocument.Parse(
            schema.Insert(1, "\"$id\":\"https://contracts.test/sample/v1\","));
        using var parsed = JsonDocument.Parse(instance);
        return document.Validate(parsed.RootElement);
    }
}
