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
    public static MessageContractAttribute? For(Type type) =>
        Cache.GetOrAdd(type, static t => t.GetCustomAttribute<MessageContractAttribute>(inherit: false));

    /// <summary>
    /// Returns the contract schema <c>$id</c>s a consumer of <paramref name="type"/> accepts: its
    /// <see cref="MessageContractAttribute.Schema"/> plus <see cref="MessageContractAttribute.Reads"/>,
    /// normalized. Empty when the type declares no schema.
    /// </summary>
    /// <exception cref="InvalidOperationException">The type lists <c>Reads</c> without a <c>Schema</c>.</exception>
    public static FrozenSet<string> SchemasFor(Type type) =>
        SchemaCache.GetOrAdd(type, static t =>
        {
            var contract = For(t);
            if (contract is null)
                return FrozenSet<string>.Empty;

            if (contract.Schema is null)
            {
                return contract.Reads is { Length: > 0 }
                    ? throw new InvalidOperationException(
                        $"'{t.Name}' lists [MessageContract] Reads without a Schema. Declare the type's own Schema too.")
                    : FrozenSet<string>.Empty;
            }

            var schemas = new List<string> { ContractSchemaId.Normalize(contract.Schema) };
            foreach (var read in contract.Reads ?? [])
                schemas.Add(ContractSchemaId.Normalize(read));

            return schemas.ToFrozenSet(StringComparer.Ordinal);
        });
}
