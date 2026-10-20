using Google.Protobuf.Collections;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Google.PubSub.Tests;

public class GooglePubSubAttributesTests
{
    [Fact]
    public void Headers_become_attributes()
    {
        var attributes = new MapField<string, string>();

        GooglePubSubAttributes.Write(
            new Dictionary<string, string> { ["tenant"] = "acme", [MessageHeaders.ContractVersion] = "2" }, attributes);

        attributes["tenant"].ShouldBe("acme");
        attributes[MessageHeaders.ContractVersion].ShouldBe("2");
    }

    [Fact]
    public void Reserved_keys_and_oversized_values_are_left_in_the_envelope()
    {
        var attributes = new MapField<string, string>();

        GooglePubSubAttributes.Write(
            new Dictionary<string, string>
            {
                ["googclient_reserved"] = "v",
                ["large"] = new string('x', 1025),
                [new string('k', 257)] = "v",
            },
            attributes);

        attributes.Count.ShouldBe(0);
    }

    [Fact]
    public void Received_attributes_become_headers()
    {
        var attributes = new MapField<string, string> { ["tenant"] = "acme" };

        GooglePubSubAttributes.Read(attributes)!["tenant"].ShouldBe("acme");
        GooglePubSubAttributes.Read(new MapField<string, string>()).ShouldBeNull();
    }
}
