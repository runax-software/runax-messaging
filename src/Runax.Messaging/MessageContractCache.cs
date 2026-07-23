using System.Collections.Concurrent;
using System.Reflection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging;

/// <summary>
/// Caches <see cref="MessageContractAttribute"/> lookups so the serializer and consumers avoid repeated reflection.
/// </summary>
internal static class MessageContractCache
{
    private static readonly ConcurrentDictionary<Type, MessageContractAttribute?> Cache = new();

    /// <summary>
    /// Returns the contract declared on <paramref name="type"/>, or <see langword="null"/> if it is unversioned.
    /// </summary>
    public static MessageContractAttribute? For(Type type) =>
        Cache.GetOrAdd(type, static t => t.GetCustomAttribute<MessageContractAttribute>(inherit: false));
}
