using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Resolves the <see cref="IUnroutableMessageHandler"/> to use for a given bus. A bus that
/// registered a handler (via <c>bus.OnUnroutableMessage(...)</c>) gets its own; every other bus
/// gets the built-in <see cref="DeadLetterUnroutableHandler"/> default.
/// </summary>
internal interface IUnroutableMessageHandlerProvider
{
    /// <summary>
    /// Returns the handler for the given bus.
    /// </summary>
    IUnroutableMessageHandler For(string busName);
}

/// <summary>
/// Default <see cref="IUnroutableMessageHandlerProvider"/>. Looks up a keyed
/// <see cref="IUnroutableMessageHandler"/> registered for the bus name; falls back to the default
/// handler when none is registered. Results are cached per bus name.
/// </summary>
internal sealed class UnroutableMessageHandlerProvider(
    IServiceProvider services,
    IUnroutableMessageHandler defaultHandler)
    : IUnroutableMessageHandlerProvider
{
    private readonly ConcurrentDictionary<string, IUnroutableMessageHandler> _cache = new(StringComparer.Ordinal);

    public IUnroutableMessageHandler For(string busName) =>
        _cache.GetOrAdd(busName, static (name, state) =>
            state.services.GetKeyedService<IUnroutableMessageHandler>(name) ?? state.defaultHandler,
            (services, defaultHandler));
}
