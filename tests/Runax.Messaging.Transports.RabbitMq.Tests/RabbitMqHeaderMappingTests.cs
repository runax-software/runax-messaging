using System.Text;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.RabbitMq.Tests;

public class RabbitMqHeaderMappingTests
{
    [Fact]
    public void Native_headers_are_written_to_the_amqp_headers_table()
    {
        var properties = RabbitMqTransport.CreateProperties(new Dictionary<string, string> { ["tenant"] = "acme" });

        properties.Headers.ShouldNotBeNull();
        properties.Headers!["tenant"].ShouldBe("acme");
        properties.ContentType.ShouldBe("application/json");
    }

    [Fact]
    public void A_message_without_headers_gets_no_headers_table()
    {
        RabbitMqTransport.CreateProperties(new Dictionary<string, string>()).Headers.ShouldBeNull();
    }

    [Fact]
    public void Received_long_strings_and_other_amqp_values_are_read_as_strings()
    {
        var headers = RabbitMqTransport.ReadHeaders(new Dictionary<string, object?>
        {
            // RabbitMQ.Client hands string headers back as raw bytes.
            [MessageHeaders.TraceParent] = Encoding.UTF8.GetBytes("00-abc-def-01"),
            ["retries"] = 3,
            ["absent"] = null,
        });

        headers.ShouldNotBeNull();
        headers![MessageHeaders.TraceParent].ShouldBe("00-abc-def-01");
        headers["retries"].ShouldBe("3");
        headers.ShouldNotContainKey("absent");
    }
}
