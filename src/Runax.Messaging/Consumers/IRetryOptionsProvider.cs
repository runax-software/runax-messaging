using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Resolves the <see cref="RetryOptions"/> to use for a given transport. A transport that registered a scoped
/// policy (via <c>WithRetry(...)</c> inside its builder block) gets its own; every other transport gets the
/// global policy (or the built-in defaults when none was configured).
/// </summary>
internal interface IRetryOptionsProvider
{
    /// <summary>
    /// Returns the retry policy for the transport with the given <see cref="IMessagingTransport.SystemName"/>.
    /// </summary>
    RetryOptions For(string transportName);
}

/// <summary>
/// Marker registered once per transport that configures a scoped <see cref="RetryOptions"/> via the per-broker
/// <c>WithRetry(...)</c>. The provider uses these markers to decide whether to resolve named options for a
/// transport or fall back to the global policy.
/// </summary>
internal sealed class ScopedRetryMarker
{
    public required string TransportName { get; init; }
}

/// <summary>
/// Default <see cref="IRetryOptionsProvider"/>. Returns the named <see cref="RetryOptions"/> for transports that
/// registered a scoped policy and the global policy otherwise. Results are cached per transport name.
/// </summary>
internal sealed class RetryOptionsProvider : IRetryOptionsProvider
{
    private readonly IOptionsMonitor<RetryOptions> _monitor;
    private readonly RetryOptions _global;
    private readonly HashSet<string> _scoped;
    private readonly ConcurrentDictionary<string, RetryOptions> _cache = new(StringComparer.Ordinal);

    public RetryOptionsProvider(
        IOptionsMonitor<RetryOptions> monitor,
        RetryOptions global,
        IEnumerable<ScopedRetryMarker> markers)
    {
        _monitor = monitor;
        _global = global;
        _scoped = new HashSet<string>(markers.Select(m => m.TransportName), StringComparer.Ordinal);
    }

    public RetryOptions For(string transportName) =>
        _cache.GetOrAdd(transportName, name =>
            _scoped.Contains(name) ? _monitor.Get(name) : _global);
}
