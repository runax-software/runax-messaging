namespace Runax.Messaging.Abstractions;

/// <summary>
/// Declares a message type as a contract. Applying it is optional: types without the attribute are
/// treated as unversioned and behave exactly as before.
/// </summary>
/// <remarks>
/// <para>
/// A contract is identified either by its schema — <see cref="Schema"/>, the <c>$id</c> of a JSON Schema
/// document, one per breaking line — or by an integer <see cref="Version"/>. Declaring both is a
/// configuration error: the <c>$id</c> already names the contract, so a number alongside it is a second
/// identity that nothing keeps in step with the first.
/// </para>
/// <para>
/// Prefer <see cref="Schema"/>. An integer is a label rather than a check — two services that both say
/// "version 2" can disagree on the shape and nothing notices — and it is kept only for contracts that
/// have not adopted schemas. It is removed in the next major release; see
/// <c>docs/specs/removing-integer-contract-versions.md</c>.
/// </para>
/// <para>
/// The identity travels with the payload in the envelope and as a native broker header, so consumers can
/// subscribe to a specific contract. When <see cref="Name"/> is omitted, the publish topic is the
/// contract's effective name.
/// </para>
/// </remarks>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
public sealed class MessageContractAttribute : Attribute
{
    /// <summary>
    /// Declares a contract identified by its <see cref="Schema"/>, which has to be set.
    /// </summary>
    public MessageContractAttribute()
    {
    }

    /// <summary>
    /// Declares a contract identified by an integer version, for a contract that has no schema. Removed
    /// in the next major release — set <see cref="Schema"/> instead.
    /// </summary>
    /// <param name="version">The contract version carried in the envelope and matched against consumers.</param>
    public MessageContractAttribute(int version) => Version = version;

    /// <summary>
    /// Gets the integer contract version, or <see langword="null"/> when the contract is identified by
    /// its <see cref="Schema"/>.
    /// </summary>
    public int? Version { get; }

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
    /// accept messages of those lines too, which is how a line is migrated. Requires <see cref="Schema"/>.
    /// </summary>
    public string[]? Reads { get; init; }
}
