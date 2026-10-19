using Amazon.SQS.Model;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sqs.Tests;

public class SqsMessageAttributesTests
{
    [Fact]
    public void Headers_become_string_attributes()
    {
        var attributes = SqsMessageAttributes.FromHeaders(new Dictionary<string, string> { ["tenant"] = "acme" });

        attributes.ShouldNotBeNull();
        attributes!["tenant"].DataType.ShouldBe("String");
        attributes["tenant"].StringValue.ShouldBe("acme");
    }

    [Fact]
    public void At_most_ten_attributes_are_written_most_important_first()
    {
        var headers = Enumerable.Range(0, 12).ToDictionary(i => $"custom-{i:D2}", i => "v");
        headers[MessageHeaders.ContractVersion] = "2";
        headers[MessageHeaders.TraceParent] = "00-abc-def-01";

        var attributes = SqsMessageAttributes.FromHeaders(headers);

        attributes.ShouldNotBeNull();
        attributes!.Count.ShouldBe(SqsMessageAttributes.MaxAttributes);
        attributes.ShouldContainKey(MessageHeaders.ContractVersion);
        attributes.ShouldContainKey(MessageHeaders.TraceParent);
        attributes.ShouldContainKey("custom-00");
        attributes.ShouldNotContainKey("custom-11");
    }

    [Theory]
    [InlineData("has space")]
    [InlineData("AWS.reserved")]
    [InlineData("amazon.reserved")]
    [InlineData(".leading")]
    [InlineData("trailing.")]
    [InlineData("double..period")]
    [InlineData("colon:name")]
    public void Headers_with_names_sqs_rejects_are_left_in_the_envelope(string name)
    {
        SqsMessageAttributes.FromHeaders(new Dictionary<string, string> { [name] = "v" }).ShouldBeNull();
    }

    [Fact]
    public void Empty_values_are_left_in_the_envelope()
    {
        SqsMessageAttributes.FromHeaders(new Dictionary<string, string> { ["tenant"] = "" }).ShouldBeNull();
    }

    [Fact]
    public void Received_string_and_number_attributes_become_headers_and_binary_ones_are_skipped()
    {
        var headers = SqsMessageAttributes.ToHeaders(new Dictionary<string, MessageAttributeValue>
        {
            ["tenant"] = new() { DataType = "String", StringValue = "acme" },
            ["retries"] = new() { DataType = "Number", StringValue = "3" },
            ["blob"] = new() { DataType = "Binary", BinaryValue = new MemoryStream([1, 2]) },
        });

        headers.ShouldNotBeNull();
        headers!["tenant"].ShouldBe("acme");
        headers["retries"].ShouldBe("3");
        headers.ShouldNotContainKey("blob");
    }
}
