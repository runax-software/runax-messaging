namespace Runax.Messaging.Abstractions;

/// <summary>
/// Well-known bus names.
/// </summary>
public static class BusNames
{
    /// <summary>
    /// The name of the bus registered by the parameterless <c>AddBus</c> overload. The default
    /// bus is what an unkeyed <see cref="IBus"/> injection resolves to, so single-bus
    /// applications never reference a bus name.
    /// </summary>
    public const string Default = "default";
}
