using System.Text.Json;
using Runax.Messaging.Abstractions;
using SnsAttribute = Amazon.SimpleNotificationService.Model.MessageAttributeValue;
using SqsAttribute = Amazon.SQS.Model.MessageAttributeValue;

namespace Runax.Messaging.Transports.Aws.Sns;

/// <summary>
/// Maps native headers to SNS message attributes, and back from what the subscribed SQS queue receives:
/// SQS message attributes with raw message delivery, or the notification's <c>MessageAttributes</c> without
/// it. SNS carries at most 10 attributes per message and restricts their names and values, so headers are
/// written most important first and any that do not fit or are not representable stay in the envelope only.
/// </summary>
internal static class SnsMessageAttributes
{
    /// <summary>The most message attributes SNS delivers on one message.</summary>
    internal const int MaxAttributes = 10;

    private const int MaxNameLength = 256;

    /// <summary>Converts headers to SNS message attributes, or returns <see langword="null"/> when none fit.</summary>
    public static Dictionary<string, SnsAttribute>? FromHeaders(IReadOnlyDictionary<string, string> headers)
    {
        Dictionary<string, SnsAttribute>? attributes = null;

        foreach (var (name, value) in MessageHeaders.InPriorityOrder(headers))
        {
            if (!IsValidName(name) || !IsValidValue(value))
                continue;

            attributes ??= new Dictionary<string, SnsAttribute>(StringComparer.Ordinal);
            attributes[name] = new SnsAttribute { DataType = "String", StringValue = value };

            if (attributes.Count == MaxAttributes)
                break;
        }

        return attributes;
    }

    /// <summary>
    /// Converts the SQS message attributes of a raw-delivered message to headers. String and Number attributes
    /// carry a string value; Binary ones have no string form and are skipped.
    /// </summary>
    public static Dictionary<string, string>? ToHeaders(Dictionary<string, SqsAttribute>? attributes)
    {
        if (attributes is null || attributes.Count == 0)
            return null;

        var headers = new Dictionary<string, string>(attributes.Count, StringComparer.Ordinal);
        foreach (var (name, attribute) in attributes)
        {
            if (attribute.StringValue is { } value)
                headers[name] = value;
        }

        return headers;
    }

    /// <summary>
    /// Reads the <c>MessageAttributes</c> of an SNS notification envelope (<c>{"Type": "String", "Value": ...}</c>
    /// per attribute) into <paramref name="headers"/>. Binary attributes are skipped.
    /// </summary>
    public static void ReadNotificationAttributes(JsonElement notification, Dictionary<string, string> headers)
    {
        if (!notification.TryGetProperty("MessageAttributes", out var attributes)
            || attributes.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        foreach (var attribute in attributes.EnumerateObject())
        {
            if (attribute.Value.ValueKind == JsonValueKind.Object
                && attribute.Value.TryGetProperty("Type", out var type)
                && type.ValueKind == JsonValueKind.String
                && type.GetString() is "String" or "Number"
                && attribute.Value.TryGetProperty("Value", out var value)
                && value.ValueKind == JsonValueKind.String)
            {
                headers[attribute.Name] = value.GetString()!;
            }
        }
    }

    /// <summary>
    /// SNS attribute names: up to 256 of <c>A-Z a-z 0-9 _ - .</c>, with no leading, trailing, or doubled
    /// period, and no <c>AWS.</c> or <c>Amazon.</c> prefix.
    /// </summary>
    internal static bool IsValidName(string name)
    {
        if (name.Length is 0 or > MaxNameLength
            || name[0] == '.'
            || name[^1] == '.'
            || name.Contains("..", StringComparison.Ordinal)
            || name.StartsWith("AWS.", StringComparison.OrdinalIgnoreCase)
            || name.StartsWith("Amazon.", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        foreach (var c in name)
        {
            if (!char.IsAsciiLetterOrDigit(c) && c is not ('_' or '-' or '.'))
                return false;
        }

        return true;
    }

    /// <summary>SNS attribute values: non-empty, and only characters SNS accepts in message content.</summary>
    internal static bool IsValidValue(string value)
    {
        if (value.Length == 0)
            return false;

        foreach (var c in value)
        {
            var allowed = c is '\t' or '\n' or '\r'
                || c is >= ' ' and <= '퟿'
                || c is >= '' and <= '�'
                || char.IsSurrogate(c);
            if (!allowed)
                return false;
        }

        return true;
    }
}
