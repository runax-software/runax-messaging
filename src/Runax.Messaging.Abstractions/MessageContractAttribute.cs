namespace Runax.Messaging.Abstractions;

/// <summary>
/// Declares a message type as a versioned contract. Applying it is optional: types without the
/// attribute are treated as unversioned and behave exactly as before.
/// </summary>
/// <remarks>
/// <para>
/// The version travels with the payload in the envelope, so consumers can subscribe to a specific
/// contract version. When <see cref="Name"/> is omitted, the publish topic is the contract's effective
/// identity — messages are routed by <c>(topic, version)</c>.
/// </para>
/// <para>
/// <see cref="Schema"/> adds a JSON Schema identity: the <c>$id</c> of the contract's schema document,
/// one per breaking line. When both sides carry one, it decides routing instead of the integer.
/// </para>
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

    /// <summary>
    /// Gets the <c>$id</c> of the contract's JSON Schema document: an absolute URI, one per breaking line
    /// (e.g. <c>https://contracts.acme.dev/orders/order-placed/v2</c>). Producers write it on every
    /// message, and consumers accept messages that carry it.
    /// </summary>
    public string? Schema { get; init; }

    /// <summary>
    /// Gets the <c>$id</c>s of earlier breaking lines this type can also read. Consumers of the type then
    /// accept messages of those lines too. Requires <see cref="Schema"/>.
    /// </summary>
    public string[]? Reads { get; init; }
}
