using System.Diagnostics;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Diagnostics;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Bridges <see cref="IMessagePublisher"/> to a single <see cref="IMessagingTransport"/>,
/// handling serialization of the message into an envelope and emitting publish telemetry.
/// The target transport is chosen by the caller — <see cref="DefaultMessagePublisher"/> for the
/// default target, or <see cref="MessagePublisherFactory"/> for an explicitly named one.
/// </summary>
internal sealed class MessagePublisherAdapter(
    IMessagingTransport transport,
    IMessageSerializerProvider serializerProvider) : IMessagePublisher
{
    private readonly IMessageSerializerProvider _serializerProvider = serializerProvider;

    private IMessagingTransport Transport => transport;

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
        var serializer = _serializerProvider.For(Transport.SystemName, topic);
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
            var envelope = _serializerProvider.For(Transport.SystemName, topic).Serialize(message, carrier);
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
