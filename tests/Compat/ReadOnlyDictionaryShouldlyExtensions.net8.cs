namespace Runax.Messaging;

/// <summary>
/// Dictionary assertions for <see cref="IReadOnlyDictionary{TKey,TValue}"/>, compiled into every test
/// project for net8.0 only (see tests/Directory.Build.props).
/// <para>
/// Shouldly's own <c>ShouldContainKey</c>, <c>ShouldNotContainKey</c> and <c>ShouldContainKeyAndValue</c>
/// take an <see cref="IDictionary{TKey,TValue}"/> in its net8.0 asset; the read-only overloads only exist
/// in the net9.0 one. Message headers are exposed as read-only dictionaries, so without these the
/// assertions would have to be written differently for the oldest target framework.
/// </para>
/// <para>
/// These live in the <c>Runax.Messaging</c> namespace rather than <c>Shouldly</c> on purpose. Every test
/// namespace is nested inside it, and extension method lookup walks the enclosing namespaces before the
/// using directives, so for a receiver that implements both interfaces (a plain
/// <see cref="Dictionary{TKey,TValue}"/>, say) these are found first instead of tying with Shouldly's.
/// A type that only implements <see cref="IDictionary{TKey,TValue}"/> matches nothing here and still
/// binds to Shouldly's own assertion.
/// </para>
/// </summary>
internal static class ReadOnlyDictionaryShouldlyExtensions
{
    public static void ShouldContainKey<TKey, TValue>(
        this IReadOnlyDictionary<TKey, TValue> dictionary,
        TKey key,
        string? customMessage = null)
    {
        if (!dictionary.ContainsKey(key))
            throw new ShouldAssertException(
                customMessage ?? $"Expected the dictionary to contain key \"{key}\", but it has {Keys(dictionary)}.");
    }

    public static void ShouldNotContainKey<TKey, TValue>(
        this IReadOnlyDictionary<TKey, TValue> dictionary,
        TKey key,
        string? customMessage = null)
    {
        if (dictionary.ContainsKey(key))
            throw new ShouldAssertException(
                customMessage ?? $"Expected the dictionary not to contain key \"{key}\", but it has {Keys(dictionary)}.");
    }

    public static void ShouldContainKeyAndValue<TKey, TValue>(
        this IReadOnlyDictionary<TKey, TValue> dictionary,
        TKey key,
        TValue value,
        string? customMessage = null)
    {
        if (!dictionary.TryGetValue(key, out var actual))
            throw new ShouldAssertException(
                customMessage ?? $"Expected the dictionary to contain key \"{key}\", but it has {Keys(dictionary)}.");

        if (!EqualityComparer<TValue>.Default.Equals(actual, value))
            throw new ShouldAssertException(
                customMessage ?? $"Expected key \"{key}\" to have value \"{value}\", but it was \"{actual}\".");
    }

    private static string Keys<TKey, TValue>(IReadOnlyDictionary<TKey, TValue> dictionary) =>
        dictionary.Count == 0 ? "no keys" : $"[{string.Join(", ", dictionary.Keys)}]";
}
