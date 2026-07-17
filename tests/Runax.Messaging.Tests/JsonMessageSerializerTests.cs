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
    public void Serialize_records_the_message_clr_type()
    {
        var envelopeJson = _serializer.Serialize(new Order(3, "thing"), headers: null);

        using var document = JsonDocument.Parse(envelopeJson);
        var messageType = document.RootElement.GetProperty("MessageType").GetString();

        messageType.ShouldNotBeNull();
        messageType!.ShouldContain(typeof(Order).FullName!);
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
    public void Deserialize_throws_for_a_malformed_envelope()
    {
        Should.Throw<InvalidOperationException>(() => _serializer.Deserialize("null", "orders"));
    }
}
