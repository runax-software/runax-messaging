using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Reflection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging;

/// <summary>
/// Caches <see cref="MessageContractAttribute"/> lookups so the serializer and consumers avoid repeated reflection.
/// </summary>
internal static class MessageContractCache
{
    private static readonly ConcurrentDictionary<Type, MessageContractAttribute?> Cache = new();
    private static readonly ConcurrentDictionary<Type, FrozenSet<string>> SchemaCache = new();

    /// <summary>
    /// Returns the contract declared on <paramref name="type"/>, or <see langword="null"/> if it is unversioned.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The type declares both a <c>Schema</c> and an integer version, or lists <c>Reads</c> without a
    /// <c>Schema</c>.
    /// </exception>
    public static MessageContractAttribute? For(Type type) =>
        Cache.GetOrAdd(type, static t =>
        {
            var contract = t.GetCustomAttribute<MessageContractAttribute>(inherit: false);
            if (contract is not null)
                Validate(t, contract);

            return contract;
        });

    /// <summary>
    /// Rejects a contract with two identities, or one that reads lines without declaring its own. Both
    /// are mistakes no amount of running can recover from, so they fail the first time the type is used.
    /// </summary>
    private static void Validate(Type type, MessageContractAttribute contract)
    {
        if (contract.Schema is not null && contract.Version is { } version)
        {
            throw new InvalidOperationException(
                $"'{type.Name}' declares [MessageContract] Schema '{contract.Schema}' and version {version}. " +
                "A contract has one identity: the $id names it, so drop the integer. The integer is only for " +
                "contracts that have no schema.");
        }

        if (contract.Schema is null && contract.Reads is { Length: > 0 })
        {
            throw new InvalidOperationException(
                $"'{type.Name}' lists [MessageContract] Reads without a Schema. Declare the type's own Schema too.");
        }
    }

    /// <summary>
    /// Returns the contract schema <c>$id</c>s a consumer of <paramref name="type"/> accepts: its
    /// <see cref="MessageContractAttribute.Schema"/> plus <see cref="MessageContractAttribute.Reads"/>,
    /// normalized. Empty when the type declares no schema.
    /// </summary>
    /// <exception cref="InvalidOperationException">The contract declares two identities, or <c>Reads</c> without a <c>Schema</c>.</exception>
    public static FrozenSet<string> SchemasFor(Type type) =>
        SchemaCache.GetOrAdd(type, static t =>
        {
            var contract = For(t);
            if (contract?.Schema is null)
                return FrozenSet<string>.Empty;

            var schemas = new List<string> { ContractSchemaId.Normalize(contract.Schema) };
            foreach (var read in contract.Reads ?? [])
                schemas.Add(ContractSchemaId.Normalize(read));

            return schemas.ToFrozenSet(StringComparer.Ordinal);
        });
}
