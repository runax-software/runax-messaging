using Runax.Messaging.Contracts.JsonSchema;

using Runax.Messaging.Cli.Contracts;

namespace Runax.Messaging.Cli.Tests;

/// <summary>The spec's evolution table (§6), row by row.</summary>
public class CompatibilityCheckerTests
{
    private const string Base = """
        "type":"object","properties":{"id":{"type":"integer"},"note":{"type":["string","null"]}},"required":["id"]
        """;

    public enum Verdict
    {
        Compatible,
        Warning,
        Breaking,
    }

    public static TheoryData<string, string, string, Verdict> Changes => new()
    {
        { "annotation only", Base, Base.Replace("\"type\":\"object\"", "\"type\":\"object\",\"title\":\"Order\",\"description\":\"x\""), Verdict.Compatible },
        { "$defs restructuring", Base, """
            "type":"object","$defs":{"id":{"type":"integer"}},"properties":{"id":{"$ref":"#/$defs/id"},"note":{"type":["string","null"]}},"required":["id"]
            """, Verdict.Compatible },
        { "add an optional property", Base, Base.Replace("\"note\"", "\"tag\":{\"type\":\"string\"},\"note\""), Verdict.Compatible },
        { "remove an optional property", Base, """
            "type":"object","properties":{"id":{"type":"integer"}},"required":["id"]
            """, Verdict.Compatible },
        { "add a required property", Base, Base.Replace("\"required\":[\"id\"]", "\"required\":[\"id\",\"currency\"]").Replace("\"note\"", "\"currency\":{\"type\":\"string\"},\"note\""), Verdict.Breaking },
        { "remove a required property", Base, """
            "type":"object","properties":{"note":{"type":["string","null"]}}
            """, Verdict.Breaking },
        { "optional to required", Base, Base.Replace("\"required\":[\"id\"]", "\"required\":[\"id\",\"note\"]"), Verdict.Breaking },
        { "required to optional", Base, Base.Replace("\"required\":[\"id\"]", "\"required\":[]"), Verdict.Breaking },
        { "integer to number", Base, Base.Replace("{\"type\":\"integer\"}", "{\"type\":\"number\"}"), Verdict.Breaking },
        { "remove null", Base, Base.Replace("[\"string\",\"null\"]", "\"string\""), Verdict.Breaking },
        { "add an enum value", """
            "properties":{"s":{"enum":["a","b"]}},"required":["s"]
            """, """
            "properties":{"s":{"enum":["a","b","c"]}},"required":["s"]
            """, Verdict.Breaking },
        { "reorder enum values", """
            "properties":{"s":{"enum":["a","b"]}},"required":["s"]
            """, """
            "properties":{"s":{"enum":["b","a"]}},"required":["s"]
            """, Verdict.Compatible },
        { "add an anyOf branch", """
            "properties":{"v":{"anyOf":[{"type":"string"}]}},"required":["v"]
            """, """
            "properties":{"v":{"anyOf":[{"type":"string"},{"type":"integer"}]}},"required":["v"]
            """, Verdict.Breaking },
        { "reorder anyOf branches", """
            "properties":{"v":{"anyOf":[{"type":"string"},{"type":"integer"}]}},"required":["v"]
            """, """
            "properties":{"v":{"anyOf":[{"type":"integer"},{"type":"string"}]}},"required":["v"]
            """, Verdict.Compatible },
        { "loosen maxLength", """
            "properties":{"c":{"type":"string","maxLength":3}},"required":["c"]
            """, """
            "properties":{"c":{"type":"string","maxLength":4}},"required":["c"]
            """, Verdict.Breaking },
        { "add a format", """
            "properties":{"c":{"type":"string"}},"required":["c"]
            """, """
            "properties":{"c":{"type":"string","format":"uuid"}},"required":["c"]
            """, Verdict.Breaking },
        { "close an object", Base, Base + ",\"additionalProperties\":false", Verdict.Breaking },
        { "additionalProperties true is still open", Base, Base + ",\"additionalProperties\":true", Verdict.Compatible },
        { "change a default", """
            "properties":{"c":{"type":"string","default":"USD"}},"required":["c"]
            """, """
            "properties":{"c":{"type":"string","default":"EUR"}},"required":["c"]
            """, Verdict.Warning },
        { "change a nested item type", """
            "properties":{"lines":{"type":"array","items":{"type":"object","properties":{"qty":{"type":"integer"}},"required":["qty"]}}},"required":["lines"]
            """, """
            "properties":{"lines":{"type":"array","items":{"type":"object","properties":{"qty":{"type":"string"}},"required":["qty"]}}},"required":["lines"]
            """, Verdict.Breaking },
    };

    [Theory]
    [MemberData(nameof(Changes))]
    public void Changes_get_the_verdict_the_spec_gives(string change, string earlier, string next, Verdict expected)
    {
        var findings = CompatibilityChecker.Compare(Document(earlier), Document(next));

        var actual = findings.Any(f => f.Severity == Severity.Error) ? Verdict.Breaking
            : findings.Count > 0 ? Verdict.Warning
            : Verdict.Compatible;
        actual.ShouldBe(expected, $"{change}: {string.Join("; ", findings.Select(f => $"{f.Pointer} {f.Message}"))}");
    }

    [Fact]
    public void Findings_name_the_property_and_what_changed()
    {
        var findings = CompatibilityChecker.Compare(
            Document(Base),
            Document(Base.Replace("\"required\":[\"id\"]", "\"required\":[\"id\",\"note\"]")));

        findings.ShouldHaveSingleItem().ShouldBe(("/properties/note", Severity.Error, "optional property 'note' was made required"));
    }

    [Fact]
    public void Recursive_schemas_are_compared_without_looping()
    {
        const string recursive = """
            "$defs":{"node":{"type":"object","properties":{"value":{"type":"integer"},"next":{"$ref":"#/$defs/node"}},"required":["value"]}},"$ref":"#/$defs/node"
            """;

        CompatibilityChecker.Compare(Document(recursive), Document(recursive)).ShouldBeEmpty();
        CompatibilityChecker.Compare(Document(recursive), Document(recursive.Replace("\"integer\"", "\"string\"")))
            .ShouldContain(f => f.Severity == Severity.Error);
    }

    private static ContractSchemaDocument Document(string body) =>
        ContractSchemaDocument.Parse($$"""{"$id":"https://contracts.test/orders/order-placed/v2",{{body}}}""");
}
