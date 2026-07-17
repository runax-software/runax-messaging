using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Tests;

public class MessageContextTests
{
    public sealed record Order(int Id, string Name);

    [Fact]
    public void Deserialize_returns_the_typed_payload()
    {
        var context = new MessageContext
        {
            Topic = "orders",
            Body = """{"Id":7,"Name":"widget"}""",
            Headers = new Dictionary<string, string>(),
        };

        var order = context.Deserialize<Order>();

        order.ShouldNotBeNull();
        order!.Id.ShouldBe(7);
        order.Name.ShouldBe("widget");
    }

    [Fact]
    public void Deserialize_returns_null_for_a_json_null_body()
    {
        var context = new MessageContext
        {
            Topic = "orders",
            Body = "null",
            Headers = new Dictionary<string, string>(),
        };

        context.Deserialize<Order>().ShouldBeNull();
    }

    [Fact]
    public void Headers_are_exposed_as_provided()
    {
        var context = new MessageContext
        {
            Topic = "orders",
            Body = "{}",
            Headers = new Dictionary<string, string> { ["k"] = "v" },
        };

        context.Headers["k"].ShouldBe("v");
    }
}
