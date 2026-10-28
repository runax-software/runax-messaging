using Runax.Messaging.Cli.Contracts;

namespace Runax.Messaging.Cli.Tests;

public class CheckerTests
{
    private const string V2 = "orders/order-placed/v2.schema.json";

    private const string Placed = """
        "type":"object","properties":{"id":{"type":"integer"}},"required":["id"]
        """;

    [Fact]
    public void An_additive_change_against_git_history_passes()
    {
        using var dir = new SchemaDirectory().InitGit().WriteSchema(V2, Placed, legacyVersion: 2).Commit("v2");
        dir.WriteSchema(V2, Placed.Replace("\"id\":{", "\"note\":{\"type\":\"string\"},\"id\":{"), legacyVersion: 2);

        Check(dir).ShouldBeEmpty();
    }

    [Fact]
    public void A_breaking_change_against_git_history_fails_with_advice()
    {
        using var dir = new SchemaDirectory().InitGit().WriteSchema(V2, Placed, legacyVersion: 2).Commit("v2");
        dir.WriteSchema(V2, """
            "type":"object","properties":{"id":{"type":"integer"},"currency":{"type":"string"}},"required":["id","currency"]
            """, legacyVersion: 2);

        var finding = Check(dir).ShouldHaveSingleItem();

        finding.Severity.ShouldBe(Severity.Error);
        finding.Pointer.ShouldBe("/properties/currency");
        finding.Message.ShouldContain("required property 'currency' was added since commit");
        finding.Message.ShouldContain("mint 'https://contracts.test/orders/order-placed/v3'");
    }

    [Fact]
    public void Every_earlier_revision_is_checked_not_just_the_latest()
    {
        // r1 has an optional note string, r2 removes it (fine), r3 brings it back as an integer: fine against
        // r2, breaking against r1, whose writers may still send a string.
        using var dir = new SchemaDirectory().InitGit()
            .WriteSchema(V2, Placed.Replace("\"id\":{", "\"note\":{\"type\":\"string\"},\"id\":{"), legacyVersion: 2).Commit("r1")
            .WriteSchema(V2, Placed, legacyVersion: 2).Commit("r2");
        dir.WriteSchema(V2, Placed.Replace("\"id\":{", "\"note\":{\"type\":\"integer\"},\"id\":{"), legacyVersion: 2);

        Check(dir).ShouldContain(f => f.Pointer == "/properties/note" && f.Message.Contains("type changed from string to integer"));
    }

    [Fact]
    public void The_legacy_version_must_not_change_within_an_id()
    {
        using var dir = new SchemaDirectory().InitGit().WriteSchema(V2, Placed, legacyVersion: 2).Commit("v2");
        dir.WriteSchema(V2, Placed, legacyVersion: 3);

        Check(dir).ShouldContain(f => f.Pointer == "/x-runax-legacy-version" && f.Message.Contains("changed from 2 to 3"));
    }

    [Fact]
    public void A_new_id_has_no_history_to_break()
    {
        using var dir = new SchemaDirectory().InitGit().WriteSchema(V2, Placed, legacyVersion: 2).Commit("v2");
        dir.WriteSchema("orders/order-placed/v3.schema.json", """
            "type":"object","properties":{"id":{"type":"string"}},"required":["id"]
            """, legacyVersion: 3);

        Check(dir).ShouldBeEmpty();
    }

    [Fact]
    public void A_baseline_directory_works_without_git()
    {
        using var baseline = new SchemaDirectory().WriteSchema(V2, Placed, legacyVersion: 2);
        using var current = new SchemaDirectory().WriteSchema(V2, Placed.Replace("\"integer\"", "\"number\""), legacyVersion: 2);

        var findings = Checker.CheckBaseline(SchemaFile.FindAll(current.Root), SchemaFile.FindAll(baseline.Root));

        findings.ShouldHaveSingleItem().Message.ShouldContain("since baseline orders/order-placed/v2.schema.json");
    }

    private static List<Finding> Check(SchemaDirectory dir) =>
        Checker.CheckHistory(SchemaFile.FindAll(dir.Root), GitHistory.Open(dir.Root));
}
