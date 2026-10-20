using Amazon.SQS.Model;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Aws.Sqs;

/// <summary>
/// Maps native headers to and from SQS message attributes. SQS carries at most 10 attributes per message and
/// restricts their names and values, so headers are written most important first and any that do not fit or
/// are not representable stay in the envelope only.
/// </summary>
internal static class SqsMessageAttributes
{
    /// <summary>The most message attributes SQS accepts on one message.</summary>
    internal const int MaxAttributes = 10;

    private const int MaxNameLength = 256;

    /// <summary>Converts headers to message attributes, or returns <see langword="null"/> when none fit.</summary>
    public static Dictionary<string, MessageAttributeValue>? FromHeaders(IReadOnlyDictionary<string, string> headers)
    {
        Dictionary<string, MessageAttributeValue>? attributes = null;

        foreach (var (name, value) in MessageHeaders.InPriorityOrder(headers))
        {
            if (!IsValidName(name) || !IsValidValue(value))
                continue;

            attributes ??= new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
            attributes[name] = new MessageAttributeValue { DataType = "String", StringValue = value };

            if (attributes.Count == MaxAttributes)
                break;
        }

        return attributes;
    }

    /// <summary>
    /// Converts received message attributes to headers. String and Number attributes carry a string value;
    /// Binary ones have no string form and are skipped.
    /// </summary>
    public static Dictionary<string, string>? ToHeaders(Dictionary<string, MessageAttributeValue>? attributes)
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
    /// SQS attribute names: up to 256 of <c>A-Z a-z 0-9 _ - .</c>, with no leading, trailing, or doubled
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

    /// <summary>SQS attribute values: non-empty, and only characters SQS accepts in message content.</summary>
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
