using System.Diagnostics;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Diagnostics;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Bridges <see cref="IMessagePublisher"/> to the underlying <see cref="IMessagingTransport"/>,
/// handling serialization of the message into an envelope and emitting publish telemetry.
/// When several transports are registered, the target is chosen with <c>PublishTo</c>.
/// </summary>
internal sealed class MessagePublisherAdapter : IMessagePublisher
{
    private readonly IReadOnlyList<IMessagingTransport> _transports;
    private readonly IMessageSerializerProvider _serializerProvider;
    private readonly string? _defaultTransportName;
    private IMessagingTransport? _resolvedTransport;

    public MessagePublisherAdapter(
        IEnumerable<IMessagingTransport> transports,
        IMessageSerializerProvider serializerProvider,
        MessagingPublishOptions publishOptions)
    {
        _transports = transports as IReadOnlyList<IMessagingTransport> ?? transports.ToArray();
        _serializerProvider = serializerProvider;
        _defaultTransportName = publishOptions.DefaultTransport;
    }

    private IMessagingTransport Transport =>
        _resolvedTransport ??= PublishTargetSelector.Select(_transports, _defaultTransportName);

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
        if (messages.Count == 0)
            return;

        var carrier = new Dictionary<string, string>();

        using var activity = MessagingDiagnostics.ActivitySource.StartActivity(
            $"{topic} publish", ActivityKind.Producer);

        if (activity is not null)
        {
            activity.SetTag("messaging.system", Transport.SystemName);
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation", "publish");
            activity.SetTag("messaging.batch.message_count", messages.Count);

            DistributedContextPropagator.Current.Inject(activity, carrier, static (c, key, value) =>
                ((Dictionary<string, string>)c!)[key] = value);
        }

        var headers = carrier.Count > 0 ? carrier : null;
        var serializer = _serializerProvider.For(Transport.SystemName);
        var envelopes = new List<string>(messages.Count);
        foreach (var message in messages)
            envelopes.Add(serializer.Serialize(message, headers));

        try
        {
            await Transport.PublishBatchAsync(topic, envelopes, cancellationToken).ConfigureAwait(false);
            MessagingDiagnostics.Published.Add(messages.Count, MessagingDiagnostics.Tags(Transport.SystemName, topic));
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
        var carrier = headers is null
            ? new Dictionary<string, string>()
            : new Dictionary<string, string>(headers);

        using var activity = MessagingDiagnostics.ActivitySource.StartActivity(
            $"{topic} publish", ActivityKind.Producer);

        if (activity is not null)
        {
            activity.SetTag("messaging.system", Transport.SystemName);
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation", "publish");

            // Propagate the current trace context to consumers through the envelope headers.
            DistributedContextPropagator.Current.Inject(activity, carrier, static (c, key, value) =>
                ((Dictionary<string, string>)c!)[key] = value);
        }

        try
        {
            var envelope = _serializerProvider.For(Transport.SystemName).Serialize(message, carrier);
            await Transport.PublishAsync(topic, envelope, cancellationToken).ConfigureAwait(false);
            MessagingDiagnostics.Published.Add(1, MessagingDiagnostics.Tags(Transport.SystemName, topic));
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }
}
