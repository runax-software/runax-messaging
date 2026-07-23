using System.Text.Json;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class JsonMessageSerializerTests
{
    public sealed record Order(int Id, string Name);
    public sealed record Nested(Order Order, IReadOnlyList<string> Tags);

    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public void Serialize_then_Deserialize_round_trips_body_and_headers()
    {
        var headers = new Dictionary<string, string> { ["correlation-id"] = "abc" };

        var envelopeJson = _serializer.Serialize(new Order(1, "widget"), headers);
        var context = _serializer.Deserialize(envelopeJson, "orders");

        context.Topic.ShouldBe("orders");
        context.Headers["correlation-id"].ShouldBe("abc");

        var order = context.Deserialize<Order>();
        order.ShouldNotBeNull();
        order!.Id.ShouldBe(1);
        order.Name.ShouldBe("widget");
    }

    [Fact]
    public void Serialize_with_null_headers_produces_empty_headers()
    {
        var envelopeJson = _serializer.Serialize(new Order(2, "gadget"), headers: null);
        var context = _serializer.Deserialize(envelopeJson, "orders");

        context.Headers.ShouldBeEmpty();
    }

    [Fact]
    public void Serialize_places_the_payload_at_the_top_level_with_metadata_under_the_reserved_key()
    {
        var envelopeJson = _serializer.Serialize(new Order(3, "thing"), headers: null);

        using var document = JsonDocument.Parse(envelopeJson);
        var root = document.RootElement;

        // Payload fields are at the top level so foreign readers see a normal object.
        root.GetProperty("Id").GetInt32().ShouldBe(3);
        root.GetProperty("Name").GetString().ShouldBe("thing");
        // Framework metadata lives under the reserved key.
        root.TryGetProperty(JsonMessageSerializer.MetadataKey, out _).ShouldBeTrue();
    }

    [Fact]
    public void Deserialize_reads_a_foreign_payload_that_has_no_metadata_key_as_raw_body()
    {
        // An S3 event (or any producer outside this library) has no reserved key.
        const string foreign = """{"Records":[{"eventSource":"aws:s3"}]}""";

        var context = _serializer.Deserialize(foreign, "s3-events");

        context.ContractVersion.ShouldBeNull();
        context.Headers.ShouldBeEmpty();
        context.Body.ShouldBe(foreign);
    }

    [Fact]
    public void Serialize_throws_for_a_non_object_payload()
    {
        Should.Throw<InvalidOperationException>(() => _serializer.Serialize(42, headers: null));
    }

    [Fact]
    public void Serialize_round_trips_nested_payloads()
    {
        var payload = new Nested(new Order(9, "boxed"), ["a", "b"]);

        var envelopeJson = _serializer.Serialize(payload, headers: null);
        var context = _serializer.Deserialize(envelopeJson, "nested");
        var result = context.Deserialize<Nested>();

        result.ShouldNotBeNull();
        result!.Order.Id.ShouldBe(9);
        result.Tags.ShouldBe(["a", "b"]);
    }

    [Fact]
    public void Deserialize_throws_for_unparseable_json()
    {
        Should.Throw<JsonException>(() => _serializer.Deserialize("{ not json", "orders"));
    }

    [Fact]
    public void EnrichHeaders_merges_headers_and_preserves_the_body()
    {
        var envelopeJson = _serializer.Serialize(new Order(4, "boxed"), new Dictionary<string, string>
        {
            ["correlation-id"] = "abc",
        });

        var enriched = _serializer.EnrichHeaders(envelopeJson, new Dictionary<string, string>
        {
            ["x-runax-dlq-reason"] = "boom",
            ["correlation-id"] = "overwritten",
        });

        var context = _serializer.Deserialize(enriched, "orders");
        context.Headers["correlation-id"].ShouldBe("overwritten");
        context.Headers["x-runax-dlq-reason"].ShouldBe("boom");

        var order = context.Deserialize<Order>();
        order.ShouldNotBeNull();
        order!.Id.ShouldBe(4);
        order.Name.ShouldBe("boxed");
    }

    [Fact]
    public void EnrichHeaders_throws_for_unparseable_json()
    {
        Should.Throw<JsonException>(
            () => _serializer.EnrichHeaders("{ not json", new Dictionary<string, string>()));
    }
}
