using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Resolves the <see cref="IMessageSerializer"/> to use for a given transport. A transport that registered a
/// scoped body serializer (via <c>UseSerializer&lt;T&gt;()</c> or <c>ConfigureSerialization(...)</c> inside its
/// builder block) gets its own; every other transport gets the global default. The reserved <c>__runax</c>
/// envelope is identical either way.
/// </summary>
internal interface IMessageSerializerProvider
{
    /// <summary>
    /// Returns the serializer for the transport with the given <see cref="IMessagingTransport.SystemName"/>.
    /// </summary>
    IMessageSerializer For(string transportName);
}

/// <summary>
/// Default <see cref="IMessageSerializerProvider"/>. Looks up a keyed <see cref="ISerializer"/> registered for
/// the transport name and wraps it in an <see cref="EnvelopeSerializer"/>; falls back to the global serializer
/// when none is registered. Results are cached per transport name.
/// </summary>
internal sealed class MessageSerializerProvider(IServiceProvider services, IMessageSerializer defaultSerializer)
    : IMessageSerializerProvider
{
    private readonly ConcurrentDictionary<string, IMessageSerializer> _cache = new(StringComparer.Ordinal);

    public IMessageSerializer For(string transportName) =>
        _cache.GetOrAdd(transportName, static (name, state) =>
        {
            var body = state.services.GetKeyedService<ISerializer>(name);
            return body is null ? state.defaultSerializer : new EnvelopeSerializer(body);
        }, (services, defaultSerializer));
}
