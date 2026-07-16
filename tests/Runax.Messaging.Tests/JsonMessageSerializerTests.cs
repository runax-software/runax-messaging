using Runax.Messaging;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class JsonMessageSerializerTests
{
    public sealed record Order(int Id, string Name);

    [Fact]
    public void Serialize_then_Deserialize_round_trips_body_and_headers()
    {
        var serializer = new JsonMessageSerializer();
        var headers = new Dictionary<string, string> { ["correlation-id"] = "abc" };

        var envelopeJson = serializer.Serialize(new Order(1, "widget"), headers);
        var context = serializer.Deserialize(envelopeJson, "orders");

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
        var serializer = new JsonMessageSerializer();

        var envelopeJson = serializer.Serialize(new Order(2, "gadget"), headers: null);
        var context = serializer.Deserialize(envelopeJson, "orders");

        context.Headers.ShouldBeEmpty();
    }
}
