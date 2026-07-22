using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Aws.Sns;

namespace Runax.Messaging.Transports.Aws.Sns.Tests;

/// <summary>
/// Integration tests for the SNS transport (SNS→SQS fan-out).
/// Requires the floci emulator (SNS + SQS) on http://localhost:4566 (or AWS_SERVICE_URL) — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SnsIntegrationTests : IAsyncLifetime
{
    private static string ServiceUrl => Environment.GetEnvironmentVariable("AWS_SERVICE_URL") ?? "http://localhost:4566";
    private static string Region => Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1";

    private AmazonSimpleNotificationServiceClient _sns = null!;
    private AmazonSQSClient _sqs = null!;
    private string _topicName = null!;
    private string _topicArn = null!;
    private string _queueUrl = null!;

    public async ValueTask InitializeAsync()
    {
        var snsConfig = new AmazonSimpleNotificationServiceConfig { ServiceURL = ServiceUrl, AuthenticationRegion = Region };
        var sqsConfig = new AmazonSQSConfig { ServiceURL = ServiceUrl, AuthenticationRegion = Region };
        _sns = new AmazonSimpleNotificationServiceClient("test", "test", snsConfig);
        _sqs = new AmazonSQSClient("test", "test", sqsConfig);

        _topicName = $"runax-test-{Guid.NewGuid():N}";
        _topicArn = (await _sns.CreateTopicAsync(_topicName)).TopicArn;
        _queueUrl = (await _sqs.CreateQueueAsync(_topicName)).QueueUrl;

        // Subscribe the queue to the topic (SubscribeQueueAsync also sets the queue policy).
        // Left as non-raw delivery so the round-trip exercises the transport's envelope unwrapping.
        await _sns.SubscribeQueueAsync(_topicArn, _sqs, _queueUrl);
    }

    public async ValueTask DisposeAsync()
    {
        await _sqs.DeleteQueueAsync(_queueUrl);
        await _sns.DeleteTopicAsync(_topicArn);
        _sqs.Dispose();
        _sns.Dispose();
    }

    private ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSns(o =>
        {
            o.Region = Region;
            o.ServiceUrl = ServiceUrl;
            o.AccessKey = "test";
            o.SecretKey = "test";
            o.WaitTimeSeconds = 1;
            o.MaxNumberOfMessages = 1;
            o.TopicArnMap[_topicName] = _topicArn;
            o.TopicQueueUrlMap[_topicName] = _queueUrl;
        }));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Publish_fans_out_through_sns_to_the_subscribed_queue()
    {
        using var provider = BuildProvider();
        var transport = provider.GetRequiredService<IMessagingTransport>();

        var received = new TaskCompletionSource<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var subscription = transport.SubscribeAsync([_topicName], (json, _) =>
        {
            received.TrySetResult(json);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
        await transport.PublishAsync(_topicName, envelope);

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(25));
        result.ShouldBe(envelope);

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

    [Fact]
    public async Task Health_check_reports_healthy()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddSns(o =>
        {
            o.Region = Region;
            o.ServiceUrl = ServiceUrl;
            o.AccessKey = "test";
            o.SecretKey = "test";
        }));
        services.AddHealthChecks().AddSnsTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Healthy);
    }
}
