namespace Runax.Messaging;

/// <summary>
/// A bus's contract-routing settings, captured from <see cref="BusBuilder"/> when its <c>AddBus</c> block
/// completes and registered as a keyed service under the bus name.
/// </summary>
internal sealed record BusContractSettings(bool RequireContractIdentity, SchemaMismatch Mismatch)
{
    /// <summary>The settings of a bus that configured nothing.</summary>
    public static BusContractSettings Default { get; } = new(RequireContractIdentity: false, SchemaMismatch.Reject);
}
