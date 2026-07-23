namespace Runax.Messaging.Abstractions;

/// <summary>
/// Declares a message type as a versioned contract. Applying it is optional: types without the
/// attribute are treated as unversioned and behave exactly as before.
/// </summary>
/// <remarks>
/// The version travels with the payload in the envelope, so consumers can subscribe to a specific
/// contract version. When <see cref="Name"/> is omitted, the publish topic is the contract's effective
/// identity — messages are routed by <c>(topic, version)</c>.
/// </remarks>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
public sealed class MessageContractAttribute(int version) : Attribute
{
    /// <summary>
    /// Gets the contract version carried in the envelope and matched against consumers.
    /// </summary>
    public int Version { get; } = version;

    /// <summary>
    /// Gets an optional stable contract name. When not set, the publish topic identifies the contract.
    /// </summary>
    public string? Name { get; init; }
}
