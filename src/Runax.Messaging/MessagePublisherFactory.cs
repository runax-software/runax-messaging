using System.Collections.Concurrent;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Serialization;

namespace Runax.Messaging;

/// <summary>
/// Default <see cref="IMessagePublisherFactory"/>. Hands out an <see cref="MessagePublisherAdapter"/> pinned
/// to a named transport, cached per system name so repeated <see cref="ForTransport"/> calls reuse the same
/// publisher. Publishers go straight to the transport and do not route through the outbox.
/// </summary>
internal sealed class MessagePublisherFactory : IMessagePublisherFactory
{
    private readonly IReadOnlyList<IMessagingTransport> _transports;
    private readonly IMessageSerializerProvider _serializerProvider;
    private readonly ConcurrentDictionary<string, IMessagePublisher> _cache = new(StringComparer.Ordinal);

    public MessagePublisherFactory(
        IEnumerable<IMessagingTransport> transports,
        IMessageSerializerProvider serializerProvider)
    {
        _transports = transports as IReadOnlyList<IMessagingTransport> ?? transports.ToArray();
        _serializerProvider = serializerProvider;
    }

    public IMessagePublisher ForTransport(string systemName) =>
        _cache.GetOrAdd(
            systemName,
            static (name, state) => new MessagePublisherAdapter(
                PublishTargetSelector.SelectByName(state._transports, name),
                state._serializerProvider),
            this);
}
