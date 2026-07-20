using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Sqs;

namespace Runax.Messaging.Sqs.Tests;

/// <summary>
/// Integration tests for the SQS transport.
/// Requires an SQS-compatible emulator (floci) on http://localhost:4566 — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SqsTransportIntegrationTests : IAsyncLifetime, IDisposable
{
    private static string ServiceUrl => Environment.GetEnvironmentVariable("AWS_SERVICE_URL") ?? "http://localhost:4566";
    private static string Region => Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1";

    private readonly record struct Order(int Id, string Tag);

    private AmazonSQSClient _reader = null!;
    private ServiceProvider _provider = null!;
    private string _topic = null!;
    private string _queueUrl = null!;

    public async ValueTask InitializeAsync()
    {
        var config = new AmazonSQSConfig { ServiceURL = ServiceUrl, AuthenticationRegion = Region };
        _reader = new AmazonSQSClient("test", "test", config);

        _topic = $"runax-test-{Guid.NewGuid():N}";
        var created = await _reader.CreateQueueAsync(_topic);
        _queueUrl = created.QueueUrl;

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSqs(o =>
        {
            o.Region = Region;
            o.ServiceUrl = ServiceUrl;
            o.AccessKey = "test";
            o.SecretKey = "test";
            o.WaitTimeSeconds = 1;
            o.MaxNumberOfMessages = 1;
        }));
        _provider = services.BuildServiceProvider();
    }

    public async ValueTask DisposeAsync()
    {
        await _reader.DeleteQueueAsync(_queueUrl);
    }

    public void Dispose()
    {
        _provider.Dispose();
        _reader.Dispose();
    }

    [Fact]
    public async Task Publisher_sends_the_serialized_message_to_the_queue()
    {
        var tag = Guid.NewGuid().ToString("N");
        var publisher = _provider.GetRequiredService<IMessagePublisher>();

        await publisher.PublishAsync(_topic, new Order(1, tag));

        var response = await _reader.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = _queueUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 5,
        });

        response.Messages.ShouldNotBeNull();
        response.Messages.Count.ShouldBe(1);
        response.Messages[0].Body.ShouldContain(tag);
    }

    [Fact]
    public async Task Transport_round_trips_publish_to_subscribe()
    {
        var transport = _provider.GetRequiredService<IMessagingTransport>();
        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";

        await transport.PublishAsync(_topic, envelope);

        var received = new TaskCompletionSource<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var subscription = transport.SubscribeAsync([_topic], (json, _) =>
        {
            received.TrySetResult(json);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(15));
        result.ShouldBe(envelope);

        await cts.CancelAsync();
        await subscription;
    }
}
