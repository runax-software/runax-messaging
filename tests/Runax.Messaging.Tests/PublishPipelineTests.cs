using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class PublishPipelineTests
{
    public sealed record Order(int Id);

    [Fact]
    public async Task PublishAsync_serializes_the_message_and_forwards_it_to_the_transport()
    {
        var transport = Substitute.For<IMessagingTransport>();
        transport.SystemName.Returns("fake");

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new FakeTransportConfig(transport))));
        await using var provider = services.BuildServiceProvider();

        await provider.GetRequiredService<IBus>().PublishAsync("orders", new Order(1));

        await transport.Received(1).PublishAsync(
            "orders",
            Arg.Is<string>(envelope => envelope.Contains(EnvelopeSerializer.MetadataKey) && envelope.Contains("\"Id\":1")),
            Arg.Any<CancellationToken>());
    }
}
