using System.Globalization;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Maps message metadata to and from native broker headers. On publish, a message's headers and its
/// contract name and version become <see cref="TransportMessage.Headers"/> (the envelope keeps its own copy).
/// On receive, the broker's headers are merged into the <see cref="MessageContext"/> decoded from the
/// envelope, with a bus's <see cref="HeaderSource"/> deciding which copy wins on a conflict.
/// </summary>
internal static class NativeHeaders
{
    /// <summary>
    /// Builds the native headers for an outgoing message: its headers plus its contract metadata.
    /// </summary>
    public static IReadOnlyDictionary<string, string> Build(
        string? contractName,
        int? contractVersion,
        string? contractSchema,
        IEnumerable<KeyValuePair<string, string>>? headers)
    {
        var native = new Dictionary<string, string>(StringComparer.Ordinal);

        if (headers is not null)
        {
            foreach (var (key, value) in headers)
                native[key] = value;
        }

        if (contractName is not null)
            native[MessageHeaders.ContractName] = contractName;

        if (contractVersion is { } version)
            native[MessageHeaders.ContractVersion] = version.ToString(CultureInfo.InvariantCulture);

        if (contractSchema is not null)
            native[MessageHeaders.ContractSchema] = ContractSchemaId.Normalize(contractSchema);

        return native;
    }

    /// <summary>
    /// Builds the native headers for an outgoing message of a <c>[MessageContract]</c> type.
    /// </summary>
    public static IReadOnlyDictionary<string, string> Build(
        MessageContractAttribute? contract,
        IEnumerable<KeyValuePair<string, string>>? headers) =>
        Build(contract?.Name, contract?.Version, contract?.Schema, headers);

    /// <summary>
    /// Builds the native headers for an already-serialized envelope (e.g. one stored by the outbox) from the
    /// metadata it carries.
    /// </summary>
    public static IReadOnlyDictionary<string, string> FromEnvelope(string envelopeJson)
    {
        var metadata = EnvelopeSerializer.ReadMetadata(envelopeJson);
        return Build(metadata.ContractName, metadata.ContractVersion, metadata.ContractSchema, metadata.Headers);
    }

    /// <summary>
    /// Merges a received message's native headers into the context decoded from its envelope. Headers are
    /// merged key by key, and so are the contract name and version (read from <see cref="MessageHeaders"/>'s
    /// contract headers, which are not repeated in <see cref="MessageContext.Headers"/>); where both copies
    /// exist, <paramref name="precedence"/> picks the winner.
    /// </summary>
    public static MessageContext Merge(
        MessageContext context,
        IReadOnlyDictionary<string, string> native,
        HeaderSource precedence)
    {
        if (native.Count == 0)
            return context;

        string? nativeName = null;
        int? nativeVersion = null;
        string? nativeSchema = null;
        var nativeHeaders = new Dictionary<string, string>(native.Count, StringComparer.Ordinal);

        foreach (var (key, value) in native)
        {
            switch (key)
            {
                case MessageHeaders.ContractName:
                    nativeName = value;
                    break;
                case MessageHeaders.ContractSchema:
                    nativeSchema = ContractSchemaId.NormalizeOrNull(value);
                    break;
                case MessageHeaders.ContractVersion:
                    if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var version))
                        nativeVersion = version;
                    break;
                default:
                    nativeHeaders[key] = value;
                    break;
            }
        }

        var brokerWins = precedence == HeaderSource.Broker;

        // Copy the losing side first so the winning side overwrites it.
        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in brokerWins ? context.Headers : nativeHeaders)
            headers[key] = value;
        foreach (var (key, value) in brokerWins ? nativeHeaders : context.Headers)
            headers[key] = value;

        return new MessageContext
        {
            Topic = context.Topic,
            Body = context.Body,
            Headers = headers,
            ContractName = brokerWins ? nativeName ?? context.ContractName : context.ContractName ?? nativeName,
            ContractVersion = brokerWins
                ? nativeVersion ?? context.ContractVersion
                : context.ContractVersion ?? nativeVersion,
            ContractSchema = brokerWins
                ? nativeSchema ?? context.ContractSchema
                : context.ContractSchema ?? nativeSchema,
            Serializer = context.Serializer,
        };
    }
}
