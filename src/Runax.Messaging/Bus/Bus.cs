using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Diagnostics;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Default <see cref="IBus"/>. Publishing runs a small pipeline: mode guard
/// (<see cref="BusMode.ConsumeOnly"/> throws), envelope serialization via the bus's serializer,
/// then the sink — the bus's transport by default, or the outbox store when an outbox is
/// configured. The message's headers and contract metadata travel both in the envelope and, unless the bus
/// turned it off, as native broker headers. Emits publish telemetry tagged with the bus name.
/// </summary>
internal sealed class Bus(
    string name,
    BusMode mode,
    BusHeaderSettings headerSettings,
    IServiceProvider services,
    IMessageSerializerProvider serializerProvider) : IBus
{
    private IMessagingTransport? _transport;
    private IBusPublishSink? _sink;

    public string Name { get; } = name;

    public BusMode Mode { get; } = mode;

    internal IMessagingTransport Transport =>
        _transport ??= services.GetRequiredKeyedService<IMessagingTransport>(Name);

    private IBusPublishSink Sink =>
        _sink ??= services.GetKeyedService<IBusPublishSink>(Name) ?? new TransportPublishSink(Transport);

    /// <inheritdoc />
    public ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        CancellationToken cancellationToken = default) =>
        PublishInternalAsync(topic, message, headers: null, cancellationToken);

    /// <inheritdoc />
    public ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default) =>
        PublishInternalAsync(topic, message, headers, cancellationToken);

    /// <inheritdoc />
    public async ValueTask PublishBatchAsync<TMessage>(
        string topic,
        IReadOnlyList<TMessage> messages,
        CancellationToken cancellationToken = default)
    {
        EnsureCanPublish();

        if (messages.Count == 0)
            return;

        var carrier = new Dictionary<string, string>();

        using var activity = MessagingDiagnostics.ActivitySource.StartActivity(
            $"{topic} publish", ActivityKind.Producer);

        if (activity is not null)
        {
            SetPublishTags(activity, topic);
            activity.SetTag("messaging.batch.message_count", messages.Count);

            DistributedContextPropagator.Current.Inject(activity, carrier, static (c, key, value) =>
                ((Dictionary<string, string>)c!)[key] = value);
        }

        var headers = carrier.Count > 0 ? carrier : null;
        var serializer = serializerProvider.For(Name, topic);
        var nativeHeaders = NativeHeadersFor<TMessage>(headers);
        var envelopes = new List<TransportMessage>(messages.Count);
        foreach (var message in messages)
            envelopes.Add(new TransportMessage(topic, serializer.Serialize(message, headers), nativeHeaders));

        try
        {
            await Sink.PublishBatchAsync(envelopes, cancellationToken).ConfigureAwait(false);
            MessagingDiagnostics.Published.Add(
                messages.Count, MessagingDiagnostics.Tags(Transport.SystemName, topic, Name));
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    private async ValueTask PublishInternalAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string>? headers,
        CancellationToken cancellationToken)
    {
        EnsureCanPublish();

        var carrier = headers is null
            ? new Dictionary<string, string>()
            : new Dictionary<string, string>(headers);

        using var activity = MessagingDiagnostics.ActivitySource.StartActivity(
            $"{topic} publish", ActivityKind.Producer);

        if (activity is not null)
        {
            SetPublishTags(activity, topic);

            // Propagate the current trace context to consumers through the envelope headers.
            DistributedContextPropagator.Current.Inject(activity, carrier, static (c, key, value) =>
                ((Dictionary<string, string>)c!)[key] = value);
        }

        try
        {
            var envelope = serializerProvider.For(Name, topic).Serialize(message, carrier);
            await Sink.PublishAsync(
                new TransportMessage(topic, envelope, NativeHeadersFor<TMessage>(carrier)), cancellationToken)
                .ConfigureAwait(false);
            MessagingDiagnostics.Published.Add(1, MessagingDiagnostics.Tags(Transport.SystemName, topic, Name));
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    private IReadOnlyDictionary<string, string>? NativeHeadersFor<TMessage>(IDictionary<string, string>? headers)
    {
        if (!headerSettings.WriteNativeHeaders)
            return null;

        return NativeHeaders.Build(MessageContractCache.For(typeof(TMessage)), headers);
    }

    private void EnsureCanPublish()
    {
        if (Mode == BusMode.ConsumeOnly)
            throw new InvalidOperationException($"Bus '{Name}' is ConsumeOnly; publishing on it is not allowed.");
    }

    private void SetPublishTags(Activity activity, string topic)
    {
        activity.SetTag("messaging.system", Transport.SystemName);
        activity.SetTag("messaging.destination.name", topic);
        activity.SetTag("messaging.operation", "publish");
        activity.SetTag("messaging.runax.bus", Name);
    }

    /// <summary>The default sink: messages go straight to the bus's transport.</summary>
    private sealed class TransportPublishSink(IMessagingTransport transport) : IBusPublishSink
    {
        public ValueTask PublishAsync(TransportMessage message, CancellationToken cancellationToken) =>
            transport.PublishAsync(message, cancellationToken);

        public ValueTask PublishBatchAsync(IReadOnlyList<TransportMessage> messages, CancellationToken cancellationToken) =>
            transport.PublishBatchAsync(messages, cancellationToken);
    }
}
