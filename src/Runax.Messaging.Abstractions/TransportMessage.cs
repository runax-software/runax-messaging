using System.Collections.ObjectModel;

namespace Runax.Messaging.Abstractions;

/// <summary>
/// A message as it crosses the transport boundary: the topic, the serialized envelope, and the headers
/// that travel natively next to it (AMQP headers, Kafka record headers, SQS message attributes, ...).
/// </summary>
/// <remarks>
/// The envelope always carries the same metadata under its reserved <c>__runax</c> key, so native headers
/// are an addition, never a replacement: a transport that cannot carry some or all of them loses nothing.
/// On receive, <see cref="Headers"/> holds the broker's user-level headers, which the framework merges with
/// the envelope's.
/// </remarks>
public sealed class TransportMessage
{
    /// <summary>
    /// Creates a transport message.
    /// </summary>
    /// <param name="topic">The topic the message is published to or was received from.</param>
    /// <param name="body">The serialized envelope.</param>
    /// <param name="headers">The native headers; <see langword="null"/> for none.</param>
    public TransportMessage(string topic, string body, IReadOnlyDictionary<string, string>? headers = null)
    {
        ArgumentNullException.ThrowIfNull(topic);
        ArgumentNullException.ThrowIfNull(body);

        Topic = topic;
        Body = body;
        Headers = headers ?? ReadOnlyDictionary<string, string>.Empty;
    }

    /// <summary>Gets the topic the message is published to or was received from.</summary>
    public string Topic { get; }

    /// <summary>Gets the serialized envelope.</summary>
    public string Body { get; }

    /// <summary>Gets the native headers. Empty when the message carries none.</summary>
    public IReadOnlyDictionary<string, string> Headers { get; }
}
