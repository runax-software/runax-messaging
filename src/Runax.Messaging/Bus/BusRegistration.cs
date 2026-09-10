using Runax.Messaging.Abstractions;

namespace Runax.Messaging;

/// <summary>
/// Descriptor recorded per <c>AddBus</c> call. Drives duplicate-name detection, unkeyed
/// <see cref="IBus"/> resolution, and <see cref="IBusProvider"/> enumeration.
/// </summary>
internal sealed class BusRegistration
{
    /// <summary>Gets the bus name.</summary>
    public required string Name { get; init; }

    /// <summary>Gets the bus's declared mode.</summary>
    public required BusMode Mode { get; init; }

    /// <summary>Gets the transport's <see cref="IMessagingTransport.SystemName"/> (telemetry/diagnostics).</summary>
    public required string SystemName { get; init; }
}
