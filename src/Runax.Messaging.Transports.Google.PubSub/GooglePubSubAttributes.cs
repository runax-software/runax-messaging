using System.Text;
using Google.Protobuf.Collections;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Google.PubSub;

/// <summary>
/// Maps native headers to and from Pub/Sub message attributes. Pub/Sub carries at most 100 attributes, with
/// keys up to 256 bytes (not starting with <c>goog</c>) and values up to 1024 bytes, so headers are written
/// most important first and any that do not fit stay in the envelope only.
/// </summary>
internal static class GooglePubSubAttributes
{
    /// <summary>The most attributes Pub/Sub accepts on one message.</summary>
    internal const int MaxAttributes = 100;

    private const int MaxKeyBytes = 256;
    private const int MaxValueBytes = 1024;

    /// <summary>Copies the headers that fit into <paramref name="attributes"/>.</summary>
    public static void Write(IReadOnlyDictionary<string, string> headers, MapField<string, string> attributes)
    {
        foreach (var (key, value) in MessageHeaders.InPriorityOrder(headers))
        {
            if (attributes.Count == MaxAttributes)
                break;

            if (IsValidKey(key) && Encoding.UTF8.GetByteCount(value) <= MaxValueBytes)
                attributes[key] = value;
        }
    }

    /// <summary>Converts received attributes to headers.</summary>
    public static Dictionary<string, string>? Read(MapField<string, string> attributes) =>
        attributes.Count == 0 ? null : new Dictionary<string, string>(attributes, StringComparer.Ordinal);

    internal static bool IsValidKey(string key) =>
        key.Length > 0
        && !key.StartsWith("goog", StringComparison.OrdinalIgnoreCase)
        && Encoding.UTF8.GetByteCount(key) <= MaxKeyBytes;
}
