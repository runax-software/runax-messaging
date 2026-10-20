using System.Globalization;
using System.Text;

namespace Runax.Messaging.Abstractions;

/// <summary>
/// Names of the native headers Runax writes next to the envelope, plus helpers for transports that map
/// them onto a broker's header model. Producers outside this library (Go, Java, Node, ...) can set the same
/// headers to version a message or continue a trace without touching the body.
/// </summary>
public static class MessageHeaders
{
    /// <summary>The contract name of a <c>[MessageContract]</c> message.</summary>
    public const string ContractName = "runax-contract-name";

    /// <summary>The contract version of a <c>[MessageContract]</c> message, as an invariant-culture integer.</summary>
    public const string ContractVersion = "runax-contract-version";

    /// <summary>The W3C trace context <c>traceparent</c> header.</summary>
    public const string TraceParent = "traceparent";

    /// <summary>The W3C trace context <c>tracestate</c> header.</summary>
    public const string TraceState = "tracestate";

    private static readonly string[] PriorityNames = [ContractName, ContractVersion, TraceParent, TraceState];

    /// <summary>
    /// Returns <paramref name="headers"/> most important first: contract metadata, then trace context, then
    /// the rest ordered by name. For brokers that cap how many headers a message may carry (SQS and SNS allow
    /// 10), write them in this order and stop at the cap; whatever does not fit still travels in the envelope.
    /// </summary>
    /// <param name="headers">The headers to order.</param>
    /// <returns>The same headers, most important first.</returns>
    public static IEnumerable<KeyValuePair<string, string>> InPriorityOrder(IReadOnlyDictionary<string, string> headers)
    {
        ArgumentNullException.ThrowIfNull(headers);

        foreach (var name in PriorityNames)
        {
            if (headers.TryGetValue(name, out var value))
                yield return new KeyValuePair<string, string>(name, value);
        }

        foreach (var header in headers
                     .Where(h => Array.IndexOf(PriorityNames, h.Key) < 0)
                     .OrderBy(h => h.Key, StringComparer.Ordinal))
        {
            yield return header;
        }
    }

    /// <summary>
    /// Converts a received broker header value to its string form: strings as-is, raw bytes as UTF-8,
    /// booleans as <c>true</c>/<c>false</c>, and numbers and other formattable values in the invariant culture.
    /// </summary>
    /// <param name="value">The broker's header value.</param>
    /// <returns>The string form, or <see langword="null"/> for a null or structured value that has none.</returns>
    public static string? FormatValue(object? value) => value switch
    {
        null => null,
        string text => text,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
        bool flag => flag ? "true" : "false",
        IFormattable formattable => formattable.ToString(format: null, CultureInfo.InvariantCulture),
        _ => null,
    };
}
