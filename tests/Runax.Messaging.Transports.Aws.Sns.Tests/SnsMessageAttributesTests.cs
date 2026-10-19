using Runax.Messaging.Abstractions;
using SqsMessage = Amazon.SQS.Model.Message;
using SqsAttribute = Amazon.SQS.Model.MessageAttributeValue;

namespace Runax.Messaging.Transports.Aws.Sns.Tests;

public class SnsMessageAttributesTests
{
    [Fact]
    public void At_most_ten_attributes_are_written_most_important_first()
    {
        var headers = Enumerable.Range(0, 12).ToDictionary(i => $"custom-{i:D2}", i => "v");
        headers[MessageHeaders.ContractName] = "orders.placed";

        var attributes = SnsMessageAttributes.FromHeaders(headers);

        attributes.ShouldNotBeNull();
        attributes!.Count.ShouldBe(SnsMessageAttributes.MaxAttributes);
        attributes[MessageHeaders.ContractName].StringValue.ShouldBe("orders.placed");
        attributes.ShouldNotContainKey("custom-11");
    }

    [Fact]
    public void Headers_are_read_from_sqs_attributes_with_raw_delivery()
    {
        var message = new SqsMessage
        {
            Body = """{"Id":1}""",
            MessageAttributes = new Dictionary<string, SqsAttribute>
            {
                ["tenant"] = new() { DataType = "String", StringValue = "acme" },
            },
        };

        var received = SnsTransport.ToTransportMessage(message, "orders");

        received.Body.ShouldBe("""{"Id":1}""");
        received.Headers["tenant"].ShouldBe("acme");
    }

    [Fact]
    public void Headers_are_read_from_the_notification_without_raw_delivery()
    {
        var message = new SqsMessage
        {
            Body = """
                {
                  "Type": "Notification",
                  "Message": "{\"Id\":1}",
                  "MessageAttributes": {
                    "tenant": { "Type": "String", "Value": "acme" },
                    "retries": { "Type": "Number", "Value": "3" },
                    "blob": { "Type": "Binary", "Value": "AQI=" }
                  }
                }
                """,
        };

        var received = SnsTransport.ToTransportMessage(message, "orders");

        received.Body.ShouldBe("""{"Id":1}""");
        received.Headers["tenant"].ShouldBe("acme");
        received.Headers["retries"].ShouldBe("3");
        received.Headers.ShouldNotContainKey("blob");
    }
}
