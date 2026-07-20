using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class InMemoryEndToEndTests
{
    private sealed record OrderPlaced(int Id);

    private sealed class Capture
    {
        public TaskCompletionSource<OrderPlaced> Received { get; } = new();
    }

    private sealed class OrderPlacedConsumer(Capture capture) : MessageConsumer<OrderPlaced>
    {
        public override string Topic => "orders.placed";

        protected override ValueTask HandleAsync(OrderPlaced message, CancellationToken cancellationToken)
        {
            capture.Received.TrySetResult(message);
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task Published_message_is_delivered_to_the_registered_consumer()
    {
        var capture = new Capture();

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(capture);
        builder.Services.AddRunaxMessaging(m => m
            .AddInMemory()
            .AddConsumer<OrderPlacedConsumer>());

        using var host = builder.Build();
        await host.StartAsync();

        var publisher = host.Services.GetRequiredService<IMessagePublisher>();
        await publisher.PublishAsync("orders.placed", new OrderPlaced(42));

        var received = await capture.Received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        received.Id.ShouldBe(42);

        await host.StopAsync();
    }
}
