using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Aws.Sqs;

namespace Runax.Messaging.Transports.Aws.Sqs.Tests;

/// <summary>
/// Integration tests for SQS visibility handling and the DeadLetter disposition.
/// Requires an SQS-compatible emulator (floci) on http://localhost:4566 — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SqsVisibilityIntegrationTests : IAsyncLifetime, IDisposable
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
        var created = await _reader.CreateQueueAsync(_topic);
        _queueUrl = created.QueueUrl;
    }

    public async ValueTask DisposeAsync() => await _reader.DeleteQueueAsync(_queueUrl);

    public void Dispose() => _reader.Dispose();

    private ServiceProvider BuildProvider(Action<SqsOptions> configure)
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
            o.MaxNumberOfMessages = 1;
            configure(o);
        })));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task DeadLetter_disposition_leaves_the_message_on_the_queue()
    {
        using var provider = BuildProvider(o =>
        {
            o.VisibilityTimeoutSeconds = 1;
            o.ExtendVisibilityDuringProcessing = false;
        });
        var transport = provider.GetRequiredService<IMessagingTransport>();

        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
        await transport.PublishAsync(_topic, envelope);

        var handled = new TaskCompletionSource();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var subscription = transport.SubscribeAsync([_topic], (_, _) =>
        {
            handled.TrySetResult();
            return ValueTask.FromResult(MessageDisposition.DeadLetter);
        }, cts.Token);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        await subscription;

        // The message was rejected, not deleted; once its visibility lapses it can be read again.
        ReceiveMessageResponse? redelivered = null;
        for (var attempt = 0; attempt < 10 && (redelivered?.Messages?.Count ?? 0) == 0; attempt++)
        {
            redelivered = await _reader.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = _queueUrl,
                WaitTimeSeconds = 2,
            });
        }

        redelivered!.Messages.ShouldNotBeNull();
        redelivered.Messages.Count.ShouldBe(1);
        redelivered.Messages[0].Body.ShouldBe(envelope);
    }

    [Fact]
    public async Task Visibility_is_extended_while_a_slow_consumer_processes()
    {
        using var provider = BuildProvider(o =>
        {
            o.VisibilityTimeoutSeconds = 2;
            o.ExtendVisibilityDuringProcessing = true;
        });
        var transport = provider.GetRequiredService<IMessagingTransport>();

        await transport.PublishAsync(_topic, $$"""{"probe":"{{Guid.NewGuid():N}}"}""");

        var started = new TaskCompletionSource();
        var finished = new TaskCompletionSource();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var subscription = transport.SubscribeAsync([_topic], async (_, _) =>
        {
            started.TrySetResult();
            await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
            finished.TrySetResult();
            return MessageDisposition.Acknowledge;
        }, cts.Token);

        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Probe for ~4s (beyond the 2s base visibility). Extension should keep the message hidden.
        var competingReceipts = 0;
        for (var attempt = 0; attempt < 4; attempt++)
        {
            var response = await _reader.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = _queueUrl,
                WaitTimeSeconds = 1,
            });
            competingReceipts += response.Messages?.Count ?? 0;
        }

        competingReceipts.ShouldBe(0);

        await finished.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await cts.CancelAsync();
        await subscription;
    }
}
