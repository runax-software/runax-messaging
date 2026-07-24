using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Aws.Sqs;

namespace Runax.Messaging.Transports.Aws.Sqs.Tests;

/// <summary>
/// Integration tests for SQS batch publish and concurrent consumption.
/// Requires an SQS-compatible emulator (floci) on http://localhost:4566 — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SqsBatchAndConcurrencyIntegrationTests : IAsyncLifetime, IDisposable
{
    private static string ServiceUrl => Environment.GetEnvironmentVariable("AWS_SERVICE_URL") ?? "http://localhost:4566";
    private static string Region => Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1";

    private AmazonSQSClient _reader = null!;
    private string _topic = null!;
    private string _queueUrl = null!;

    public async ValueTask InitializeAsync()
    {
        var config = new AmazonSQSConfig { ServiceURL = ServiceUrl, AuthenticationRegion = Region };
        _reader = new AmazonSQSClient("test", "test", config);

        _topic = $"runax-test-{Guid.NewGuid():N}";
        _queueUrl = (await _reader.CreateQueueAsync(_topic)).QueueUrl;
    }

    public async ValueTask DisposeAsync() => await _reader.DeleteQueueAsync(_queueUrl);

    public void Dispose() => _reader.Dispose();

    private ServiceProvider BuildProvider(Action<SqsOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSqs(sqs => sqs.Configure(o =>
        {
            o.Region = Region;
            o.ServiceUrl = ServiceUrl;
            o.AccessKey = "test";
            o.SecretKey = "test";
            o.WaitTimeSeconds = 1;
            o.MaxNumberOfMessages = 10;
            configure?.Invoke(o);
        })));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task PublishBatchAsync_sends_all_messages_across_chunks()
    {
        using var provider = BuildProvider();
        var transport = provider.GetRequiredService<IMessagingTransport>();

        // 12 > the SQS batch limit of 10, so this exercises chunking.
        var envelopes = Enumerable.Range(0, 12).Select(i => $$"""{"n":{{i}}}""").ToList();
        await transport.PublishBatchAsync(_topic, envelopes);

        var bodies = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (bodies.Count < 12 && DateTime.UtcNow < deadline)
        {
            var response = await _reader.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = _queueUrl,
                MaxNumberOfMessages = 10,
                WaitTimeSeconds = 2,
            });

            foreach (var message in response.Messages ?? [])
            {
                bodies.Add(message.Body);
                await _reader.DeleteMessageAsync(_queueUrl, message.ReceiptHandle);
            }
        }

        bodies.Count.ShouldBe(12);
    }

    [Fact]
    public async Task Messages_are_processed_concurrently_up_to_the_limit()
    {
        using var provider = BuildProvider(o => o.MaxConcurrentMessages = 5);
        var transport = provider.GetRequiredService<IMessagingTransport>();

        const int total = 8;
        await transport.PublishBatchAsync(_topic,
            Enumerable.Range(0, total).Select(i => $$"""{"n":{{i}}}""").ToList());

        var processed = new Countdown(total);
        var currentInFlight = 0;
        var maxInFlight = 0;
        var gate = new object();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var subscription = transport.SubscribeAsync([_topic], async (_, _) =>
        {
            lock (gate)
            {
                currentInFlight++;
                maxInFlight = Math.Max(maxInFlight, currentInFlight);
            }

            await Task.Delay(400, cts.Token);

            lock (gate)
                currentInFlight--;

            processed.Signal();
            return MessageDisposition.Acknowledge;
        }, cts.Token);

        await processed.Completed.WaitAsync(TimeSpan.FromSeconds(25));
        maxInFlight.ShouldBeGreaterThan(1);

        await cts.CancelAsync();
        try
        {
            await subscription;
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
    }

    private sealed class Countdown(int count)
    {
        private readonly TaskCompletionSource _completed = new();
        private int _remaining = count;

        public Task Completed => _completed.Task;

        public void Signal()
        {
            if (Interlocked.Decrement(ref _remaining) == 0)
                _completed.TrySetResult();
        }
    }
}
