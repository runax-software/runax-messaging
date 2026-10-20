namespace Runax.Messaging;

/// <summary>
/// A bus's native-header settings, captured from <see cref="BusBuilder"/> when its <c>AddBus</c> block
/// completes and registered as a keyed service under the bus name.
/// </summary>
internal sealed record BusHeaderSettings(bool WriteNativeHeaders, HeaderSource Precedence)
{
    /// <summary>The settings of a bus that configured nothing.</summary>
    public static BusHeaderSettings Default { get; } = new(WriteNativeHeaders: true, HeaderSource.Broker);
}
