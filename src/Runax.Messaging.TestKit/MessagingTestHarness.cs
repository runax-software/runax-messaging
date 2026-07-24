using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.TestKit;

/// <summary>
/// A broker-free test host for Runax.Messaging consumers. It wires up a real dependency-injection container
/// and hosted dispatch pipeline over the built-in in-memory transport, so tests can
/// <see cref="PublishAsync{TMessage}(string, TMessage, CancellationToken)">publish</see> a message and then
/// assert what a consumer received, how many times, and whether it was retried or dead-lettered — with no
/// running broker.
/// </summary>
/// <remarks>
/// Build one with <see cref="MessagingTestHarness.Create"/>, register the consumers under test and their
/// dependencies, then <see cref="MessagingTestHarnessBuilder.StartAsync"/>. Dispose the harness (ideally with
/// <c>await using</c>) to stop the host. A harness is single-use and not designed to be restarted.
/// </remarks>
public sealed class MessagingTestHarness : IAsyncDisposable
{
    private static readonly TimeSpan DefaultWaitTimeout = TimeSpan.FromSeconds(5);

    private readonly IHost _host;
    private readonly MessageRecorder _recorder;
    private bool _disposed;

    private MessagingTestHarness(IHost host, MessageRecorder recorder)
    {
        _host = host;
        _recorder = recorder;
    }

    /// <summary>
    /// Gets the running host's service provider, so tests can resolve consumers, dependencies, or the
    /// <see cref="IMessagePublisher"/> directly.
    /// </summary>
    public IServiceProvider Services => _host.Services;

    /// <summary>
    /// Gets every message the harness has observed being delivered so far, in the order it saw them. Includes
    /// messages that were dead-lettered (which reappear on the <c>&lt;topic&gt;.dead-letter</c> topic).
    /// </summary>
    public IReadOnlyList<RecordedMessage> Delivered => _recorder.Messages;

    /// <summary>
    /// Starts a new builder for configuring and starting a harness.
    /// </summary>
    /// <returns>A fresh <see cref="MessagingTestHarnessBuilder"/>.</returns>
    public static MessagingTestHarnessBuilder Create() => new();

    internal static async Task<MessagingTestHarness> StartAsync(
        IReadOnlyList<Action<MessagingConfigurator>> configureMessaging,
        IReadOnlyList<Action<IServiceCollection>> configureServices,
        CancellationToken cancellationToken)
    {
        var recorder = new MessageRecorder();

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(recorder);

        foreach (var configure in configureServices)
            configure(builder.Services);

        builder.Services.AddRunaxMessaging(configurator =>
        {
            // Always register the in-memory transport so a harness with only top-level consumers still works.
            configurator.AddInMemory();

            foreach (var configure in configureMessaging)
                configure(configurator);
        });

        WrapTransportWithRecorder(builder.Services);

        var host = builder.Build();
        var harness = new MessagingTestHarness(host, recorder);

        try
        {
            await host.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await harness.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return harness;
    }

    /// <summary>
    /// Publishes a message through the harness's <see cref="IMessagePublisher"/>, exactly as application code
    /// would. Registered consumers for the topic are dispatched asynchronously; await one of the
    /// <c>WaitFor…</c> methods to observe the result.
    /// </summary>
    /// <typeparam name="TMessage">The message payload type.</typeparam>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="message">The message payload.</param>
    /// <param name="cancellationToken">Token to cancel the publish.</param>
    /// <returns>A task that completes once the message has been handed to the transport.</returns>
    public ValueTask PublishAsync<TMessage>(string topic, TMessage message, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return Services.GetRequiredService<IMessagePublisher>().PublishAsync(topic, message, cancellationToken);
    }

    /// <summary>
    /// Publishes a message with custom headers through the harness's <see cref="IMessagePublisher"/>.
    /// </summary>
    /// <typeparam name="TMessage">The message payload type.</typeparam>
    /// <param name="topic">The topic to publish to.</param>
    /// <param name="message">The message payload.</param>
    /// <param name="headers">Transport-level headers to attach.</param>
    /// <param name="cancellationToken">Token to cancel the publish.</param>
    /// <returns>A task that completes once the message has been handed to the transport.</returns>
    public ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return Services.GetRequiredService<IMessagePublisher>().PublishAsync(topic, message, headers, cancellationToken);
    }

    /// <summary>
    /// Waits until a message is delivered on <paramref name="topic"/> (and acknowledged, i.e. handled by a
    /// consumer) and returns it. Use this after <see cref="PublishAsync{TMessage}(string, TMessage, CancellationToken)"/>
    /// to assert what a consumer received.
    /// </summary>
    /// <param name="topic">The topic to wait on.</param>
    /// <param name="timeout">How long to wait before giving up. Defaults to 5 seconds.</param>
    /// <param name="cancellationToken">Token to cancel the wait.</param>
    /// <returns>The first acknowledged message observed on the topic.</returns>
    public Task<RecordedMessage> WaitForConsumedAsync(
        string topic,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default) =>
        WaitAsync(
            m => m.Topic == topic && m.Disposition == MessageDisposition.Acknowledge,
            $"a consumed message on topic '{topic}'",
            timeout,
            cancellationToken);

    /// <summary>
    /// Waits until a message of type <typeparamref name="TMessage"/> is consumed on <paramref name="topic"/>
    /// and returns the deserialized payload.
    /// </summary>
    /// <typeparam name="TMessage">The expected message type.</typeparam>
    /// <param name="topic">The topic to wait on.</param>
    /// <param name="timeout">How long to wait before giving up. Defaults to 5 seconds.</param>
    /// <param name="cancellationToken">Token to cancel the wait.</param>
    /// <returns>The deserialized message that was consumed.</returns>
    public async Task<TMessage> WaitForConsumedAsync<TMessage>(
        string topic,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var recorded = await WaitForConsumedAsync(topic, timeout, cancellationToken).ConfigureAwait(false);
        return recorded.As<TMessage>()
               ?? throw new InvalidOperationException(
                   $"The message consumed on topic '{topic}' could not be deserialized to {typeof(TMessage).Name}.");
    }

    /// <summary>
    /// Waits until a message is framework-dead-lettered from <paramref name="topic"/> — that is, it reappears on
    /// <c>&lt;topic&gt;&lt;suffix&gt;</c> (the suffix defaults to <c>.dead-letter</c>) — and returns it. Requires the default
    /// framework-managed dead-letter strategy (the harness uses it unless you override it via
    /// <see cref="MessagingTestHarnessBuilder.ConfigureMessaging"/>).
    /// </summary>
    /// <param name="topic">The original topic the message was published to.</param>
    /// <param name="deadLetterSuffix">The dead-letter topic suffix. Defaults to <c>.dead-letter</c>.</param>
    /// <param name="timeout">How long to wait before giving up. Defaults to 5 seconds.</param>
    /// <param name="cancellationToken">Token to cancel the wait.</param>
    /// <returns>The dead-lettered message.</returns>
    public Task<RecordedMessage> WaitForDeadLetterAsync(
        string topic,
        string deadLetterSuffix = ".dead-letter",
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var deadLetterTopic = topic + deadLetterSuffix;
        return WaitAsync(
            m => m.Topic == deadLetterTopic,
            $"a dead-lettered message on topic '{deadLetterTopic}'",
            timeout,
            cancellationToken);
    }

    /// <summary>
    /// Returns how many acknowledged (consumed) deliveries have been observed on <paramref name="topic"/> so
    /// far. Retries redeliver the same message, so a message retried twice before succeeding contributes one
    /// consumed delivery here; inspect <see cref="Delivered"/> for the raw sequence.
    /// </summary>
    /// <param name="topic">The topic to count deliveries on.</param>
    /// <returns>The number of acknowledged deliveries observed on the topic.</returns>
    public int ConsumedCount(string topic) =>
        Delivered.Count(m => m.Topic == topic && m.Disposition == MessageDisposition.Acknowledge);

    private Task<RecordedMessage> WaitAsync(
        Func<RecordedMessage, bool> predicate,
        string description,
        TimeSpan? timeout,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        return WaitWithTimeoutAsync(predicate, description, timeout ?? DefaultWaitTimeout, cancellationToken);
    }

    private async Task<RecordedMessage> WaitWithTimeoutAsync(
        Func<RecordedMessage, bool> predicate,
        string description,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);

        try
        {
            return await _recorder.WaitAsync(predicate, timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Timed out after {timeout.TotalSeconds:0.###}s waiting for {description}.");
        }
    }

    private static void WrapTransportWithRecorder(IServiceCollection services)
    {
        // AddInMemory registers the in-memory transport as IMessagingTransport. Replace that descriptor with a
        // RecordingTransport that wraps the original — built from the descriptor's own type/factory, so the
        // in-memory transport (which is internal to the core package) never has to be referenced directly.
        for (var i = 0; i < services.Count; i++)
        {
            var descriptor = services[i];
            if (descriptor.ServiceType != typeof(IMessagingTransport))
                continue;

            services[i] = ServiceDescriptor.Singleton<IMessagingTransport>(sp =>
            {
                var inner = CreateInner(sp, descriptor);
                var recorder = sp.GetRequiredService<MessageRecorder>();
                var serializer = sp.GetRequiredService<IMessageSerializer>();
                var retryOptions = sp.GetRequiredService<RetryOptions>();
                return new RecordingTransport(inner, recorder, serializer, retryOptions);
            });

            return;
        }
    }

    private static IMessagingTransport CreateInner(IServiceProvider sp, ServiceDescriptor descriptor)
    {
        if (descriptor.ImplementationInstance is IMessagingTransport instance)
            return instance;

        if (descriptor.ImplementationFactory is not null)
            return (IMessagingTransport)descriptor.ImplementationFactory(sp);

        if (descriptor.ImplementationType is not null)
            return (IMessagingTransport)ActivatorUtilities.CreateInstance(sp, descriptor.ImplementationType);

        throw new InvalidOperationException("The in-memory transport registration could not be resolved.");
    }

    /// <summary>
    /// Stops the host and disposes the harness. Safe to call more than once.
    /// </summary>
    /// <returns>A task that completes when the host has stopped.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            await _host.StopAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Shutdown timed out; still dispose the host below.
        }

        _host.Dispose();
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);
}
