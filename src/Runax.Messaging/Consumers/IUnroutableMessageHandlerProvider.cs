using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Resolves the <see cref="IUnroutableMessageHandler"/> to use for a given transport. A transport that registered
/// a scoped handler (via <c>OnUnroutableMessage(...)</c> inside its builder block) gets its own; every other
/// transport gets the global handler (or the built-in <see cref="DeadLetterUnroutableHandler"/> default).
/// </summary>
internal interface IUnroutableMessageHandlerProvider
{
    /// <summary>
    /// Returns the handler for the transport with the given <see cref="IMessagingTransport.SystemName"/>.
    /// </summary>
    IUnroutableMessageHandler For(string transportName);
}

/// <summary>
/// Default <see cref="IUnroutableMessageHandlerProvider"/>. Looks up a keyed
/// <see cref="IUnroutableMessageHandler"/> registered for the transport name; falls back to the global handler
/// when none is registered. Results are cached per transport name.
/// </summary>
internal sealed class UnroutableMessageHandlerProvider(
    IServiceProvider services,
    IUnroutableMessageHandler defaultHandler)
    : IUnroutableMessageHandlerProvider
{
    private readonly ConcurrentDictionary<string, IUnroutableMessageHandler> _cache = new(StringComparer.Ordinal);

    public IUnroutableMessageHandler For(string transportName) =>
        _cache.GetOrAdd(transportName, static (name, state) =>
            state.services.GetKeyedService<IUnroutableMessageHandler>(name) ?? state.defaultHandler,
            (services, defaultHandler));
}
