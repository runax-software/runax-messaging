using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Runax.Messaging.Abstractions;

/// <summary>
/// Default <see cref="ISerializer"/>, built on System.Text.Json. Honors the shared
/// <see cref="JsonSerializerOptions"/> configured via <c>ConfigureSerialization</c> (e.g. a naming policy
/// or a source-generated <see cref="IJsonTypeInfoResolver"/>).
/// </summary>
/// <remarks>
/// Trim- and native-AOT-safe: every call goes through the <see cref="JsonTypeInfo{T}"/> the options'
/// resolver supplies, never the reflection-only <see cref="JsonSerializer"/> overloads. Options that set no
/// resolver fall back to reflection while the app allows it (<see cref="JsonSerializer.IsReflectionEnabledByDefault"/>,
/// off in trimmed and native AOT apps); otherwise only types a source-generated context declares serialize.
/// </remarks>
internal sealed class SystemTextJsonSerializer(JsonSerializerOptions? options = null) : ISerializer
{
    /// <summary>The serializer over default options, used when no serializer was threaded through.</summary>
    internal static SystemTextJsonSerializer Default { get; } = new();

    private readonly JsonSerializerOptions _options = Freeze(options);

    /// <inheritdoc />
    public string Serialize<TMessage>(TMessage message) => JsonSerializer.Serialize(message, TypeInfo<TMessage>());

    /// <inheritdoc />
    public TMessage? Deserialize<TMessage>(string body) => JsonSerializer.Deserialize(body, TypeInfo<TMessage>());

    private JsonTypeInfo<T> TypeInfo<T>() => (JsonTypeInfo<T>)_options.GetTypeInfo(typeof(T));

    /// <summary>
    /// Returns read-only options with a resolver, so <see cref="JsonSerializerOptions.GetTypeInfo"/> caches
    /// metadata. Mutable options are copied rather than locked, leaving the caller's instance editable.
    /// </summary>
    private static JsonSerializerOptions Freeze(JsonSerializerOptions? options)
    {
        if (options is { IsReadOnly: true })
            return options;

        var frozen = options is null ? new JsonSerializerOptions() : new JsonSerializerOptions(options);
        frozen.TypeInfoResolver ??= JsonSerializer.IsReflectionEnabledByDefault
            ? CreateReflectionResolver()
            : JsonTypeInfoResolver.Combine();
        frozen.MakeReadOnly();
        return frozen;
    }

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026:RequiresUnreferencedCode",
        Justification = "Only called when JsonSerializer.IsReflectionEnabledByDefault, a feature switch that " +
            "trimmed and native AOT apps turn off.")]
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050:RequiresDynamicCode",
        Justification = "Only called when JsonSerializer.IsReflectionEnabledByDefault, a feature switch that " +
            "native AOT apps turn off.")]
    private static DefaultJsonTypeInfoResolver CreateReflectionResolver() => new();
}
