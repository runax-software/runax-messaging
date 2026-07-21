using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class BatchPublishTests
{
    private sealed record Item(int Id);

    private sealed class Collector(int expected)
    {
        private readonly TaskCompletionSource _done = new();
        private int _remaining = expected;

        public ConcurrentBag<int> Ids { get; } = [];
        public Task Done => _done.Task;

        public void Add(int id)
        {
            Ids.Add(id);
            if (Interlocked.Decrement(ref _remaining) == 0)
                _done.TrySetResult();
        }
    }

    private sealed class ItemConsumer(Collector collector) : MessageConsumer<Item>
    {
        public override string Topic => "items";

        protected override ValueTask HandleAsync(Item message, CancellationToken cancellationToken)
        {
            collector.Add(message.Id);
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task PublishBatchAsync_delivers_every_message()
    {
        var collector = new Collector(expected: 5);
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(collector);
        builder.Services.AddRunaxMessaging(m => m.AddInMemory().AddConsumer<ItemConsumer>());
        using var host = builder.Build();
        await host.StartAsync();

        var messages = Enumerable.Range(1, 5).Select(i => new Item(i)).ToList();
        await host.Services.GetRequiredService<IMessagePublisher>().PublishBatchAsync("items", messages);

        await collector.Done.WaitAsync(TimeSpan.FromSeconds(5));
        collector.Ids.OrderBy(i => i).ShouldBe([1, 2, 3, 4, 5]);

        await host.StopAsync();
    }

    [Fact]
    public async Task PublishBatchAsync_with_no_messages_is_a_no_op()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        using var provider = services.BuildServiceProvider();

        await Should.NotThrowAsync(async () =>
            await provider.GetRequiredService<IMessagePublisher>().PublishBatchAsync("items", Array.Empty<Item>()));
    }
}
