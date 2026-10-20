using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Azure.EventHubs.Tests;

public class AzureEventHubsHeaderMappingTests
{
    [Fact]
    public void Native_headers_are_written_as_event_properties()
    {
        var eventData = AzureEventHubsTransport.ToEventData(
            new TransportMessage("orders", "{}", new Dictionary<string, string> { ["tenant"] = "acme" }));

        eventData.EventBody.ToString().ShouldBe("{}");
        eventData.Properties["tenant"].ShouldBe("acme");
    }

    [Fact]
    public void Received_event_properties_are_read_as_strings()
    {
        var headers = AzureEventHubsTransport.ReadHeaders(new Dictionary<string, object>
        {
            ["tenant"] = "acme",
            ["retries"] = 3L,
        });

        headers.ShouldNotBeNull();
        headers!["tenant"].ShouldBe("acme");
        headers["retries"].ShouldBe("3");
    }
}
