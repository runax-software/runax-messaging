using Runax.Messaging.Abstractions;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class MessagePublisherAdapterTests
{
    public sealed record Order(int Id);

    [Fact]
    public async Task PublishAsync_serializes_the_message_and_forwards_it_to_the_transport()
    {
        var transport = Substitute.For<IMessagingTransport>();
        var adapter = new MessagePublisherAdapter(transport, new JsonMessageSerializer());

        await adapter.PublishAsync("orders", new Order(1));

        await transport.Received(1).PublishAsync(
            "orders",
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }
}
