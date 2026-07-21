using System.Diagnostics;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Diagnostics;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Bridges <see cref="IMessagePublisher"/> to the underlying <see cref="IMessagingTransport"/>,
/// handling serialization of the message into an envelope and emitting publish telemetry.
/// </summary>
internal sealed class MessagePublisherAdapter(IMessagingTransport transport, IMessageSerializer serializer)
    : IMessagePublisher
{
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
            activity.SetTag("messaging.system", transport.SystemName);
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation", "publish");

            // Propagate the current trace context to consumers through the envelope headers.
            DistributedContextPropagator.Current.Inject(activity, carrier, static (c, key, value) =>
                ((Dictionary<string, string>)c!)[key] = value);
        }

        try
        {
            var envelope = serializer.Serialize(message, carrier);
            await transport.PublishAsync(topic, envelope, cancellationToken).ConfigureAwait(false);
            MessagingDiagnostics.Published.Add(1, MessagingDiagnostics.Tags(transport.SystemName, topic));
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }
}
