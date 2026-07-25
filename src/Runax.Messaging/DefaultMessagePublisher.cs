using Runax.Messaging.Abstractions;

namespace Runax.Messaging;

/// <summary>
/// The <see cref="IMessagePublisher"/> applications get by default. It publishes to the single registered
/// transport, or the one chosen with <c>PublishTo</c> when several are registered, resolving that target
/// lazily on first use and then delegating to the matching <see cref="MessagePublisherFactory"/> publisher.
/// </summary>
internal sealed class DefaultMessagePublisher(
    IMessagePublisherFactory factory,
    IEnumerable<IMessagingTransport> transports,
    MessagingPublishOptions publishOptions) : IMessagePublisher
{
    private IMessagePublisher? _target;

    private IMessagePublisher Target => _target ??= factory.ForTransport(
        PublishTargetSelector.Select(
            transports as IReadOnlyList<IMessagingTransport> ?? transports.ToArray(),
            publishOptions.DefaultTransport).SystemName);

    public ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        CancellationToken cancellationToken = default) =>
        Target.PublishAsync(topic, message, cancellationToken);

    public ValueTask PublishAsync<TMessage>(
        string topic,
        TMessage message,
        IDictionary<string, string> headers,
        CancellationToken cancellationToken = default) =>
        Target.PublishAsync(topic, message, headers, cancellationToken);

    public ValueTask PublishBatchAsync<TMessage>(
        string topic,
        IReadOnlyList<TMessage> messages,
        CancellationToken cancellationToken = default) =>
        Target.PublishBatchAsync(topic, messages, cancellationToken);
}
