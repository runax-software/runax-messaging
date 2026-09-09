using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Resolves the <see cref="IMessageSerializer"/> to use for a given bus and topic. Selection runs
/// from most to least specific: a serializer registered for this exact <c>(bus, topic)</c> pair
/// (via <c>bus.UseSerializerForTopic&lt;T&gt;()</c> / <c>bus.ConfigureSerializationForTopic(...)</c>),
/// then one for the bus (<c>bus.UseSerializer&lt;T&gt;()</c> / <c>bus.ConfigureSerialization(...)</c>),
/// then the global default. The reserved <c>__runax</c> envelope is identical at every level.
/// </summary>
internal interface IMessageSerializerProvider
{
    /// <summary>
    /// Returns the serializer for the given topic on the given bus.
    /// </summary>
    IMessageSerializer For(string busName, string topic);
}

/// <summary>
/// Keyed-service key for per-topic services scoped to one bus — the single normalized scoping key
/// below the bus level.
/// </summary>
internal readonly record struct TopicKey(string Bus, string Topic);

/// <summary>
/// Default <see cref="IMessageSerializerProvider"/>. Looks up a keyed <see cref="ISerializer"/> by
/// decreasing specificity — <c>(bus, topic)</c>, then bus — and wraps the first match in an
/// <see cref="EnvelopeSerializer"/>; falls back to the global serializer when none is registered.
/// Results are cached per <c>(bus, topic)</c>.
/// </summary>
internal sealed class MessageSerializerProvider(IServiceProvider services, IMessageSerializer defaultSerializer)
    : IMessageSerializerProvider
{
    private readonly ConcurrentDictionary<TopicKey, IMessageSerializer> _cache = new();

    public IMessageSerializer For(string busName, string topic) =>
        _cache.GetOrAdd(new TopicKey(busName, topic), static (key, state) =>
        {
            var body =
                state.services.GetKeyedService<ISerializer>(key)
                ?? state.services.GetKeyedService<ISerializer>(key.Bus);
            return body is null ? state.defaultSerializer : new EnvelopeSerializer(body);
        }, (services, defaultSerializer));
}
