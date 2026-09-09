using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging;

/// <summary>
/// Default <see cref="IBusProvider"/> over the recorded <see cref="BusRegistration"/> descriptors,
/// resolving each bus from its keyed registration on first access.
/// </summary>
internal sealed class BusProvider(IServiceProvider services, IEnumerable<BusRegistration> registrations)
    : IBusProvider
{
    private readonly IReadOnlyList<BusRegistration> _registrations =
        registrations as IReadOnlyList<BusRegistration> ?? registrations.ToArray();

    private IReadOnlyList<IBus>? _buses;

    public IReadOnlyList<IBus> Buses =>
        _buses ??= _registrations
            .Select(r => services.GetRequiredKeyedService<IBus>(r.Name))
            .ToArray();

    public IBus GetBus(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        return services.GetKeyedService<IBus>(name)
            ?? throw new InvalidOperationException(
                $"No bus is registered under the name '{name}'. Registered buses: {Describe()}.");
    }

    private string Describe() =>
        _registrations.Count == 0 ? "(none)" : string.Join(", ", _registrations.Select(r => r.Name));
}
