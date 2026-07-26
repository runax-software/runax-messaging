using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Resolves the <see cref="IMessageSerializer"/> to use for a given transport and topic. Selection runs from
/// most to least specific: a serializer registered for this exact <c>(transport, topic)</c> pair, then one for
/// this topic on any transport, then one for this transport (any topic), then the global default. Each level is
/// registered through <c>UseSerializer&lt;T&gt;()</c> / <c>ConfigureSerialization(...)</c> (transport or global)
/// or their <c>*ForTopic</c> counterparts. The reserved <c>__runax</c> envelope is identical at every level.
/// </summary>
internal interface IMessageSerializerProvider
{
    /// <summary>
    /// Returns the serializer for the given topic on the transport with the given
    /// <see cref="IMessagingTransport.SystemName"/>.
    /// </summary>
    IMessageSerializer For(string transportName, string topic);
}

/// <summary>Keyed-service key for a body serializer scoped to a topic on any transport.</summary>
internal readonly record struct TopicSerializerKey(string Topic);

/// <summary>Keyed-service key for a body serializer scoped to a topic on one specific transport.</summary>
internal readonly record struct TransportTopicSerializerKey(string Transport, string Topic);

/// <summary>
/// Default <see cref="IMessageSerializerProvider"/>. Looks up a keyed <see cref="ISerializer"/> by decreasing
/// specificity — <c>(transport, topic)</c>, then topic, then transport — and wraps the first match in an
/// <see cref="EnvelopeSerializer"/>; falls back to the global serializer when none is registered. Results are
/// cached per <c>(transport, topic)</c>.
/// </summary>
internal sealed class MessageSerializerProvider(IServiceProvider services, IMessageSerializer defaultSerializer)
    : IMessageSerializerProvider
{
    private readonly ConcurrentDictionary<(string Transport, string Topic), IMessageSerializer> _cache = new();

    public IMessageSerializer For(string transportName, string topic) =>
        _cache.GetOrAdd((transportName, topic), static (key, state) =>
        {
            var body =
                state.services.GetKeyedService<ISerializer>(new TransportTopicSerializerKey(key.Transport, key.Topic))
                ?? state.services.GetKeyedService<ISerializer>(new TopicSerializerKey(key.Topic))
                ?? state.services.GetKeyedService<ISerializer>(key.Transport);
            return body is null ? state.defaultSerializer : new EnvelopeSerializer(body);
        }, (services, defaultSerializer));
}
