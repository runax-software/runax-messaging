using System.Text;
using Confluent.Kafka;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Kafka.Tests;

public class KafkaHeaderMappingTests
{
    [Fact]
    public void Native_headers_are_written_as_utf8_record_headers()
    {
        var record = KafkaTransport.ToKafkaMessage(
            new TransportMessage("orders", "{}", new Dictionary<string, string> { ["tenant"] = "acmé" }));

        record.Value.ShouldBe("{}");
        Encoding.UTF8.GetString(record.Headers.GetLastBytes("tenant")).ShouldBe("acmé");
    }

    [Fact]
    public void A_message_without_headers_gets_no_record_headers()
    {
        KafkaTransport.ToKafkaMessage(new TransportMessage("orders", "{}")).Headers.ShouldBeNull();
    }

    [Fact]
    public void Repeated_record_headers_resolve_to_the_last_value()
    {
        var headers = new Headers
        {
            { "tenant", Encoding.UTF8.GetBytes("first") },
            { "tenant", Encoding.UTF8.GetBytes("last") },
            { "empty", null },
        };

        var read = KafkaTransport.ReadHeaders(headers);

        read.ShouldNotBeNull();
        read!["tenant"].ShouldBe("last");
        read.ShouldNotContainKey("empty");
    }
}
