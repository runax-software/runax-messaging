using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.ServiceBus.Tests;

public class AzureServiceBusHeaderMappingTests
{
    [Fact]
    public void Native_headers_are_written_as_application_properties()
    {
        var message = AzureServiceBusTransport.ToServiceBusMessage(
            new TransportMessage("orders", "{}", new Dictionary<string, string> { ["tenant"] = "acme" }));

        message.Body.ToString().ShouldBe("{}");
        message.ApplicationProperties["tenant"].ShouldBe("acme");
    }

    [Fact]
    public void Received_application_properties_are_read_as_strings()
    {
        var headers = AzureServiceBusTransport.ReadHeaders(new Dictionary<string, object>
        {
            ["tenant"] = "acme",
            ["retries"] = 3,
        });

        headers.ShouldNotBeNull();
        headers!["tenant"].ShouldBe("acme");
        headers["retries"].ShouldBe("3");
    }
}
