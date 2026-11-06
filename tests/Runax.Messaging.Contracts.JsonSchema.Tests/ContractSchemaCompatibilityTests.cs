namespace Runax.Messaging.Contracts.JsonSchema.Tests;

public class ContractSchemaCompatibilityTests
{
    private const string V2 = "orders/order-placed/v2.schema.json";

    private const string Placed = """
        "type":"object","properties":{"id":{"type":"integer"}},"required":["id"]
        """;

    [Fact]
    public void An_additive_change_against_the_baseline_passes()
    {
        using var baseline = new SchemaDirectory().WriteSchema(V2, Placed, legacyVersion: 2);
        using var current = new SchemaDirectory().WriteSchema(
            V2, WithOptional("note", "string"), legacyVersion: 2);

        Compare(baseline, current).ShouldBeEmpty();
    }

    [Fact]
    public void A_breaking_change_fails_with_advice()
    {
        using var baseline = new SchemaDirectory().WriteSchema(V2, Placed, legacyVersion: 2);
        using var current = new SchemaDirectory().WriteSchema(V2, """
            "type":"object","properties":{"id":{"type":"integer"},"currency":{"type":"string"}},"required":["id","currency"]
            """, legacyVersion: 2);

        var finding = Compare(baseline, current).ShouldHaveSingleItem();

        finding.Severity.ShouldBe(ContractSchemaSeverity.Error);
        finding.File.ShouldBe(V2);
        finding.JsonPointer.ShouldBe("/properties/currency");
        finding.Message.ShouldContain("required property 'currency' was added since baseline");
        finding.Message.ShouldContain("mint 'https://contracts.test/orders/order-placed/v3'");
    }

    [Fact]
    public void The_legacy_version_must_not_change_within_an_id()
    {
        using var baseline = new SchemaDirectory().WriteSchema(V2, Placed, legacyVersion: 2);
        using var current = new SchemaDirectory().WriteSchema(V2, Placed, legacyVersion: 3);

        Compare(baseline, current)
            .ShouldContain(f => f.JsonPointer == "/x-runax-legacy-version" && f.Message.Contains("changed from 2 to 3"));
    }

    [Fact]
    public void A_new_id_has_no_history_to_break()
    {
        using var baseline = new SchemaDirectory().WriteSchema(V2, Placed, legacyVersion: 2);
        using var current = new SchemaDirectory()
            .WriteSchema(V2, Placed, legacyVersion: 2)
            .WriteSchema("orders/order-placed/v3.schema.json", """
                "type":"object","properties":{"id":{"type":"string"}},"required":["id"]
                """, legacyVersion: 3);

        Compare(baseline, current).ShouldBeEmpty();
    }

    [Fact]
    public void A_line_retired_since_the_baseline_is_not_reported()
    {
        using var baseline = new SchemaDirectory()
            .WriteSchema(V2, Placed, legacyVersion: 2)
            .WriteSchema("orders/order-cancelled/v1.schema.json", Placed, legacyVersion: 1);
        using var current = new SchemaDirectory().WriteSchema(V2, Placed, legacyVersion: 2);

        Compare(baseline, current).ShouldBeEmpty();
    }

    /// <summary>
    /// Compatibility isn't transitive, which is why the document overload exists: r3 is compatible with r2
    /// but not with r1, whose writers may still send a string, so a caller holding a line's full history
    /// has to compare against every earlier revision rather than only the newest.
    /// </summary>
    [Fact]
    public void Every_earlier_revision_is_checked_not_just_the_latest()
    {
        var r1 = Document(WithOptional("note", "string"));
        var r2 = Document(Placed);
        var r3 = Document(WithOptional("note", "integer"));

        ContractSchemaCompatibility.Compare(r2, r3, "r2").ShouldBeEmpty();

        ContractSchemaCompatibility.Compare(r1, r3, "r1")
            .ShouldContain(f =>
                f.JsonPointer == "/properties/note" && f.Message.Contains("type changed from string to integer since r1"));
    }

    [Fact]
    public void A_revision_declaring_another_id_is_not_part_of_the_line()
    {
        var other = ContractSchemaDocument.Parse(
            $$"""{"$id":"https://contracts.test/orders/order-cancelled/v1",{{Placed}}}""");

        ContractSchemaCompatibility.Compare(other, Document(Placed.Replace("integer", "string")))
            .ShouldBeEmpty();
    }

    private static ContractSchemaReport Compare(SchemaDirectory baseline, SchemaDirectory current) =>
        ContractSchemaCompatibility.Compare(baseline.Root, current.Root);

    private static ContractSchemaDocument Document(string body) =>
        ContractSchemaDocument.Parse($$"""{"$id":"https://contracts.test/orders/order-placed/v2",{{body}}}""");

    /// <summary>The base shape with one extra optional property of the given type.</summary>
    private static string WithOptional(string name, string type) =>
        Placed.Replace("\"id\":{", $"\"{name}\":{{\"type\":\"{type}\"}},\"id\":{{", StringComparison.Ordinal);
}
