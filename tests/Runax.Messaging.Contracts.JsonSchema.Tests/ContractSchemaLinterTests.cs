namespace Runax.Messaging.Contracts.JsonSchema.Tests;

public class ContractSchemaLinterTests
{
    private const string Placed = """
        "type":"object","properties":{"kind":{"const":"order.placed"},"id":{"type":"integer"}},"required":["kind","id"]
        """;

    [Fact]
    public void A_valid_set_of_files_has_no_findings()
    {
        using var dir = new SchemaDirectory()
            .WriteSchema("orders/order-placed/v2.schema.json", Placed)
            .WriteSchema("orders/order-placed/v3.schema.json", Placed);

        Lint(dir).ShouldBeEmpty();
    }

    [Fact]
    public void The_schema_draft_must_be_2020_12()
    {
        using var dir = new SchemaDirectory().Write(
            "orders/order-placed/v2.schema.json",
            """{"$schema":"http://json-schema.org/draft-07/schema#","$id":"https://contracts.test/orders/order-placed/v2","required":["id"]}""");

        Lint(dir).ShouldContain(f => f.Severity == ContractSchemaSeverity.Error && f.Message.Contains("'$schema' must be"));
    }

    [Fact]
    public void The_file_path_must_match_the_id()
    {
        using var dir = new SchemaDirectory().Write(
            "orders/order-placed/v2.schema.json",
            $$"""{"$schema":"{{SchemaDirectory.Draft}}","$id":"https://contracts.test/orders/order-placed/v3",{{Placed}}}""");

        Lint(dir).ShouldContain(f => f.JsonPointer == "/$id" && f.Message.Contains("must match the $id"));
    }

    [Fact]
    public void Unknown_keywords_are_errors_but_custom_annotations_are_not()
    {
        using var dir = new SchemaDirectory().WriteSchema(
            "orders/order-placed/v2.schema.json",
            """
                "type":"object","properties":{"id":{"type":"integer","x-owner":"orders"}},"requried":["id"]
                """);

        var findings = Lint(dir);

        findings.ShouldContain(f => f.JsonPointer == "/requried" && f.Message.Contains("unknown keyword 'requried'"));
        findings.ShouldNotContain(f => f.Message.Contains("x-owner"));
    }

    [Fact]
    public void Keywords_outside_the_profile_are_errors()
    {
        using var dir = new SchemaDirectory().WriteSchema(
            "orders/order-placed/v2.schema.json", """
                "required":["id"],"not":{"required":["x"]}
                """);

        Lint(dir).ShouldContain(f => f.Severity == ContractSchemaSeverity.Error && f.Message.Contains("'not' is not part of the contract profile"));
    }

    [Fact]
    public void The_root_object_must_stay_open()
    {
        using var dir = new SchemaDirectory().WriteSchema(
            "orders/order-placed/v2.schema.json", """
                "required":["id"],"additionalProperties":false
                """);

        Lint(dir).ShouldContain(f => f.JsonPointer == "/additionalProperties" && f.Severity == ContractSchemaSeverity.Error);
    }

    [Fact]
    public void Ids_must_be_unique()
    {
        using var dir = new SchemaDirectory()
            .WriteSchema("a/orders/order-placed/v2.schema.json", Placed)
            .Write("b/orders/order-placed/v2.schema.json",
                $$"""{"$schema":"{{SchemaDirectory.Draft}}","$id":"https://contracts.test/a/orders/order-placed/v2",{{Placed}}}""");

        Lint(dir).ShouldContain(f => f.Message.Contains("is already declared by a/orders/order-placed/v2.schema.json"));
    }

    [Fact]
    public void Unchecked_formats_and_non_selective_lines_are_warnings()
    {
        using var dir = new SchemaDirectory().WriteSchema(
            "misc/anything/v1.schema.json", """
                "type":"object","properties":{"contact":{"type":"string","format":"email"}}
                """);

        var findings = Lint(dir);

        findings.ShouldAllBe(f => f.Severity == ContractSchemaSeverity.Warning);
        findings.ShouldContain(f => f.JsonPointer == "/properties/contact/format" && f.Message.Contains("isn't checked"));
        findings.ShouldContain(f => f.Message.Contains("accepts any JSON object"));
    }

    [Fact]
    public void Invalid_json_is_an_error()
    {
        using var dir = new SchemaDirectory().Write("orders/order-placed/v2.schema.json", "{ not json");

        Lint(dir).ShouldHaveSingleItem().Message.ShouldStartWith("not valid JSON");
    }

    private static ContractSchemaReport Lint(SchemaDirectory dir) => ContractSchemaLinter.Lint(dir.Root);
}
