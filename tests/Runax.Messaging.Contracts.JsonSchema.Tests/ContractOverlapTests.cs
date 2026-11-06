using Runax.Messaging.Contracts.JsonSchema;

namespace Runax.Messaging.Contracts.JsonSchema.Tests;

public class ContractOverlapTests
{
    [Fact]
    public void Lines_with_different_required_discriminators_are_disjoint()
    {
        AreDisjoint(
            """{"properties":{"kind":{"const":"a"}},"required":["kind"]}""",
            """{"properties":{"kind":{"const":"b"}},"required":["kind"]}""").ShouldBeTrue();
    }

    [Fact]
    public void Lines_whose_shared_required_property_has_disjoint_types_are_disjoint()
    {
        AreDisjoint(
            """{"properties":{"id":{"type":"string"}},"required":["id"]}""",
            """{"properties":{"id":{"type":"integer"}},"required":["id"]}""").ShouldBeTrue();
    }

    [Fact]
    public void An_integer_and_a_number_overlap()
    {
        AreDisjoint(
            """{"properties":{"id":{"type":"number"}},"required":["id"]}""",
            """{"properties":{"id":{"type":"integer"}},"required":["id"]}""").ShouldBeFalse();
    }

    [Fact]
    public void A_discriminator_only_one_line_requires_does_not_separate_them()
    {
        AreDisjoint(
            """{"properties":{"kind":{"const":"a"}},"required":["kind"]}""",
            """{"properties":{"kind":{"const":"b"}}}""").ShouldBeFalse();
    }

    private static bool AreDisjoint(string a, string b) =>
        ContractOverlap.AreDisjoint(
            ContractSchemaDocument.Parse(a.Insert(1, "\"$id\":\"https://contracts.test/a\",")),
            ContractSchemaDocument.Parse(b.Insert(1, "\"$id\":\"https://contracts.test/b\",")));
}
