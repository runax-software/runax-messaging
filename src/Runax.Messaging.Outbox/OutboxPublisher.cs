using Runax.Messaging;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Outbox;

/// <summary>
/// An <see cref="IMessagePublisher"/> that serializes messages and writes them to the
/// <see cref="IOutboxStore"/> instead of publishing directly, so the write can share the caller's
/// transaction. The <see cref="OutboxDispatcher"/> later delivers them to the transport.
/// </summary>
internal sealed class OutboxPublisher(
    IMessageSerializerProvider serializerProvider,
    IEnumerable<IMessagingTransport> transports,
    MessagingPublishOptions publishOptions,
    IOutboxStore store) : IMessagePublisher
{
    private IMessagingTransport? _target;

    // The dispatcher delivers every outbox message to a single selected transport, so we serialize with that
    // transport's serializer up front — matching OutboxDispatcher's PublishTargetSelector choice.
    private IMessagingTransport Target => _target ??= PublishTargetSelector.Select(
        transports as IReadOnlyList<IMessagingTransport> ?? transports.ToArray(),
        publishOptions.DefaultTransport);

    public ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        CancellationToken cancellationToken = default) =>
        StoreAsync(topic, message, headers: null, cancellationToken);

    public ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default) =>
        StoreAsync(topic, message, headers, cancellationToken);

    public async ValueTask PublishBatchAsync<TMessage>(
        string topic,
        IReadOnlyList<TMessage> messages,
        CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
            await StoreAsync(topic, message, headers: null, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask StoreAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string>? headers,
        CancellationToken cancellationToken)
    {
        var payload = serializerProvider.For(Target.SystemName, topic).Serialize(message, headers);
        await store.AddAsync(new OutboxMessage { Topic = topic, Payload = payload }, cancellationToken)
            .ConfigureAwait(false);
    }
}
