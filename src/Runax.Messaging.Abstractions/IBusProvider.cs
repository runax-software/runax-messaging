namespace Runax.Messaging.Abstractions;

/// <summary>
/// Enumerates and resolves the application's configured buses. Prefer injecting
/// <see cref="IBus"/> (unkeyed for the default bus, keyed by name otherwise); use the
/// provider for dynamic lookups, diagnostics, and admin surfaces.
/// </summary>
public interface IBusProvider
{
    /// <summary>
    /// Returns the bus registered under the given name.
    /// </summary>
    /// <param name="name">The bus name passed to <c>AddBus</c>.</param>
    /// <returns>The bus.</returns>
    /// <exception cref="InvalidOperationException">No bus is registered under <paramref name="name"/>.</exception>
    IBus GetBus(string name);

    /// <summary>Gets every configured bus, in registration order.</summary>
    IReadOnlyList<IBus> Buses { get; }
}
