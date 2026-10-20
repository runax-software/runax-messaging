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
        services.AddRunaxMessaging(m => m.AddBus(bus => bus.AddTransport(new GooglePubSubConfig
        {
            ProjectId = ProjectId,
            TopicSubscriptionMap = { [_topicId] = _subscriptionId },
        })));
        return services.BuildServiceProvider();
    }

    private static readonly Dictionary<string, string> ProbeHeaders = new()
    {
        ["tenant"] = "acme",
        [MessageHeaders.TraceParent] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    };

    [Fact]
    public async Task Transport_round_trips_the_envelope_and_native_headers()
    {
        await using var provider = BuildProvider();
        var transport = provider.GetRequiredKeyedService<IMessagingTransport>(BusNames.Default);

        var received = new TaskCompletionSource<TransportMessage>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var subscription = transport.SubscribeAsync([_topicId], message =>
        {
            received.TrySetResult(message);
            return ValueTask.FromResult(MessageDisposition.Acknowledge);
        }, cts.Token);

        // Give the streaming pull a moment to establish before publishing.
        await Task.Delay(1000);

        var envelope = $$"""{"probe":"{{Guid.NewGuid():N}}"}""";
        await transport.PublishAsync(new TransportMessage(_topicId, envelope, ProbeHeaders));

        var result = await received.Task.WaitAsync(TimeSpan.FromSeconds(20));
        result.Body.ShouldBe(envelope);
        result.Headers.ShouldContainKeyAndValue("tenant", "acme");
        result.Headers.ShouldContainKeyAndValue(MessageHeaders.TraceParent, ProbeHeaders[MessageHeaders.TraceParent]);

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
        // The transport config auto-registers its health check as "runax:default".
        services.AddRunaxMessaging(m => m.AddBus(bus =>
            bus.AddTransport(new GooglePubSubConfig { ProjectId = ProjectId })));
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(r => r.Name == $"runax:{BusNames.Default}");

        report.Status.ShouldBe(HealthStatus.Healthy);
        report.Entries[$"runax:{BusNames.Default}"].Status.ShouldBe(HealthStatus.Healthy);
    }
}
