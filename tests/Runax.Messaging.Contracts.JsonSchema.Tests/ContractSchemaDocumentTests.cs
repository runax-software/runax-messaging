using Runax.Messaging.Contracts.JsonSchema;

namespace Runax.Messaging.Contracts.JsonSchema.Tests;

public class ContractSchemaDocumentTests
{
    [Theory]
    [InlineData("""{"not":{"type":"string"}}""", "'not'")]
    [InlineData("""{"if":{"type":"string"},"then":{"minLength":1}}""", "'if'")]
    [InlineData("""{"allOf":[{"type":"string"}]}""", "'allOf'")]
    [InlineData("""{"patternProperties":{"^x":{}}}""", "'patternProperties'")]
    [InlineData("""{"properties":{"a":{"$ref":"https://other.test/schema.json"}}}""", "only local references")]
    [InlineData("""{"properties":{"a":{"$id":"https://contracts.test/inner"}}}""", "nested '$id'")]
    [InlineData("""{"$ref":"#/$defs/missing"}""", "does not resolve")]
    public void Keywords_outside_the_contract_profile_fail_to_load(string schema, string expected)
    {
        var ex = Should.Throw<ContractSchemaException>(() =>
            ContractSchemaDocument.Parse(schema.Insert(1, "\"$id\":\"https://contracts.test/sample/v1\",")));

        ex.Message.ShouldContain(expected);
    }

    [Theory]
    [InlineData("""{"type":"object"}""")]
    [InlineData("""{"$id":"relative/v1"}""")]
    [InlineData("""{"$id":42}""")]
    [InlineData("""[1]""")]
    public void A_document_needs_an_absolute_id(string schema)
    {
        Should.Throw<ContractSchemaException>(() => ContractSchemaDocument.Parse(schema));
    }

    [Fact]
    public void The_id_and_legacy_version_are_read_and_an_empty_fragment_is_dropped()
    {
        var document = ContractSchemaDocument.Parse(
            """{"$id":"https://contracts.test/orders/v2#","x-runax-legacy-version":2,"required":["id"]}""");

        document.Id.ShouldBe("https://contracts.test/orders/v2");
        document.LegacyVersion.ShouldBe(2);
    }

    [Fact]
    public void A_line_is_selective_only_when_it_rejects_the_empty_object()
    {
        ContractSchemaDocument.Parse("""{"$id":"https://contracts.test/a","type":"object"}""").IsSelective.ShouldBeFalse();
        ContractSchemaDocument.Parse("""{"$id":"https://contracts.test/b","required":["id"]}""").IsSelective.ShouldBeTrue();
        ContractSchemaDocument.Parse(
            """{"$id":"https://contracts.test/c","anyOf":[{"required":["a"]},{"required":["b"]}]}""").IsSelective.ShouldBeTrue();
    }

    [Fact]
    public void Embedded_documents_are_loaded_from_an_assembly()
    {
        var registry = new ContractSchemaRegistry([new ContractSchemaSource.Embedded(typeof(ContractSchemaDocumentTests).Assembly)]);

        registry.Documents.Select(d => d.Id).ShouldBe(
            [
                Ids.PlacedV2,
                Ids.PlacedV3,
                Ids.CancelledV1,
                Ids.AnythingV1,
            ],
            ignoreOrder: true);
        registry.TryGet(Ids.PlacedV2 + "#", out var placed).ShouldBeTrue();
        placed!.LegacyVersion.ShouldBe(2);
    }

    [Fact]
    public void Two_different_documents_with_one_id_conflict_but_identical_ones_do_not()
    {
        const string a = """{"$id":"https://contracts.test/x","required":["a"]}""";
        const string b = """{"$id":"https://contracts.test/x","required":["b"]}""";

        Should.NotThrow(() => new ContractSchemaRegistry(
            [new ContractSchemaSource.Inline(a), new ContractSchemaSource.Inline(a)]));
        Should.Throw<ContractSchemaException>(() => new ContractSchemaRegistry(
            [new ContractSchemaSource.Inline(a), new ContractSchemaSource.Inline(b)]));
    }
}
