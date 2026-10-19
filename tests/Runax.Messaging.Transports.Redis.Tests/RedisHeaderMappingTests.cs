using Runax.Messaging.Abstractions;
using StackExchange.Redis;

namespace Runax.Messaging.Transports.Redis.Tests;

public class RedisHeaderMappingTests
{
    [Fact]
    public void The_envelope_is_the_data_field_and_each_header_is_another_field()
    {
        var fields = RedisTransport.ToStreamFields(
            new TransportMessage("orders", "{}", new Dictionary<string, string> { ["tenant"] = "acme" }));

        fields.Select(f => (f.Name.ToString(), f.Value.ToString())).ShouldBe([("data", "{}"), ("tenant", "acme")]);
    }

    [Fact]
    public void A_header_named_like_the_data_field_is_left_in_the_envelope()
    {
        var fields = RedisTransport.ToStreamFields(
            new TransportMessage("orders", "{}", new Dictionary<string, string> { ["data"] = "clash" }));

        fields.ShouldHaveSingleItem().Value.ToString().ShouldBe("{}");
    }

    [Fact]
    public void Every_field_but_data_is_read_as_a_header()
    {
        var entry = new StreamEntry("1-0", [new NameValueEntry("data", "{}"), new NameValueEntry("tenant", "acme")]);

        var headers = RedisTransport.ReadHeaders(entry);

        headers.ShouldNotBeNull();
        headers!.ShouldHaveSingleItem().ShouldBe(new KeyValuePair<string, string>("tenant", "acme"));
        RedisTransport.ReadHeaders(new StreamEntry("1-0", [new NameValueEntry("data", "{}")])).ShouldBeNull();
    }
}
