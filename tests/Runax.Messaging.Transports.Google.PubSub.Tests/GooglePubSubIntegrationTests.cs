using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Transports.Google.PubSub;

namespace Runax.Messaging.Transports.Google.PubSub.Tests;

/// <summary>
/// Integration tests for the Google Pub/Sub transport.
/// Requires the Pub/Sub emulator on localhost:8085 (or PUBSUB_EMULATOR_HOST) — see compose.yml.
/// </summary>
[Trait("Category", "Integration")]
public sealed class GooglePubSubIntegrationTests : IAsyncLifetime
{
    private const string ProjectId = "runax-test";
    private static string EmulatorHost => Environment.GetEnvironmentVariable("PUBSUB_EMULATOR_HOST") ?? "localhost:8085";

    private PublisherServiceApiClient _publisherApi = null!;
    private SubscriberServiceApiClient _subscriberApi = null!;
    private string _topicId = null!;
    private string _subscriptionId = null!;

    public async ValueTask InitializeAsync()
    {
        Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST", EmulatorHost);

        _topicId = $"runax-test-{Guid.NewGuid():N}";
        _subscriptionId = $"{_topicId}-sub";

        _publisherApi = await new PublisherServiceApiClientBuilder
        {
            EmulatorDetection = EmulatorDetection.EmulatorOrProduction
        }.BuildAsync();
        _subscriberApi = await new SubscriberServiceApiClientBuilder
        {
            EmulatorDetection = EmulatorDetection.EmulatorOrProduction
        }.BuildAsync();

        var topicName = TopicName.FromProjectTopic(ProjectId, _topicId);
        await _publisherApi.CreateTopicAsync(topicName);
        await _subscriberApi.CreateSubscriptionAsync(
            SubscriptionName.FromProjectSubscription(ProjectId, _subscriptionId),
            topicName, pushConfig: null, ackDeadlineSeconds: 60);
    }

    public async ValueTask DisposeAsync()
    {
        await _subscriberApi.DeleteSubscriptionAsync(SubscriptionName.FromProjectSubscription(ProjectId, _subscriptionId));
        await _publisherApi.DeleteTopicAsync(TopicName.FromProjectTopic(ProjectId, _topicId));
    }

    private ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddGooglePubSub(__tb => __tb.Configure(o =>
        {
            o.ProjectId = ProjectId;
            o.TopicSubscriptionMap[_topicId] = _subscriptionId;
        })));
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Transport_round_trips_publish_to_subscribe()
    {
        await using var provider = BuildProvider();
        var transport = provider.GetRequiredService<IMessagingTransport>();

        var received = new TaskCompletionSource<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var subscription = transport.SubscribeAsync([_topicId], (json, _) =>
        {
            received.TrySetResult(json);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        // Give the streaming pull a moment to establish before publishing.
        await Task.Delay(1000);

        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
        await transport.PublishAsync(_topicId, envelope);

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(20));
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
        services.AddRunaxMessaging(m => m.AddGooglePubSub(__tb => __tb.Configure(o => o.ProjectId = ProjectId)));
        services.AddHealthChecks().AddGooglePubSubTransport();
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        report.Status.ShouldBe(HealthStatus.Healthy);
    }
}
