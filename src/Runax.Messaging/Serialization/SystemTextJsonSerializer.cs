using System.Text.Json;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Default <see cref="ISerializer"/>, built on System.Text.Json. Honors the shared
/// <see cref="JsonSerializerOptions"/> configured via <c>ConfigureSerialization</c> (e.g. a naming policy
/// or a source-generated <see cref="System.Text.Json.Serialization.Metadata.IJsonTypeInfoResolver"/>).
/// </summary>
internal sealed class SystemTextJsonSerializer(JsonSerializerOptions? options = null) : ISerializer
{
    private readonly JsonSerializerOptions? _options = options;

    /// <inheritdoc />
    public string Serialize<TMessage>(TMessage message) => JsonSerializer.Serialize(message, _options);

    /// <inheritdoc />
    public TMessage? Deserialize<TMessage>(string body) => JsonSerializer.Deserialize<TMessage>(body, _options);
}
