using Runax.Messaging.Abstractions;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.TestKit;

/// <summary>
/// An <see cref="IMessagingTransport"/> decorator that wraps the in-memory transport and records the messages
/// the harness cares about, then forwards to the inner transport unchanged:
/// <list type="bullet">
///   <item><description>Every delivery it dispatches, with the disposition the pipeline returned — observed by
///   sitting inside the delivery path rather than competing for messages, so it records without stealing.</description></item>
///   <item><description>Every message the framework publishes to a dead-letter topic (the original topic plus
///   the configured suffix). No consumer subscribes to that topic, so it never flows through the delivery path;
///   catching it at publish time is how the harness surfaces dead-letters.</description></item>
/// </list>
/// </summary>
internal sealed class RecordingTransport(
    IMessagingTransport inner,
    MessageRecorder recorder,
    IMessageSerializer serializer,
    RetryOptions retryOptions) : IMessagingTransport
{
    public string SystemName => inner.SystemName;

    public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
    {
        RecordDeadLetterPublish(topic, envelopeJson);
        return inner.PublishAsync(topic, envelopeJson, cancellationToken);
    }

    public async ValueTask PublishBatchAsync(
        string topic,
        IReadOnlyList<string> envelopeJsons,
        CancellationToken cancellationToken = default)
    {
        foreach (var envelopeJson in envelopeJsons)
            RecordDeadLetterPublish(topic, envelopeJson);

        await inner.PublishBatchAsync(topic, envelopeJsons, cancellationToken).ConfigureAwait(false);
    }

    public Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default) =>
        inner.SubscribeAsync(topics, async (envelopeJson, topic) =>
        {
            var disposition = await onMessage(envelopeJson, topic).ConfigureAwait(false);
            Record(topic, envelopeJson, disposition);
            return disposition;
        }, cancellationToken);

    private void RecordDeadLetterPublish(string topic, string envelopeJson)
    {
        // Dead-letter topics are never subscribed, so their messages never reach the delivery path above.
        // Record them here so WaitForDeadLetterAsync can observe them. A normal publish is recorded when it is
        // consumed, so this only ever fires for the framework-managed dead-letter topic.
        if (retryOptions.DeadLetterTopicSuffix.Length > 0
            && topic.EndsWith(retryOptions.DeadLetterTopicSuffix, StringComparison.Ordinal))
        {
            Record(topic, envelopeJson, MessageDisposition.DeadLetter);
        }
    }

    private void Record(string topic, string envelopeJson, MessageDisposition disposition)
    {
        MessageContext context;
        try
        {
            context = serializer.Deserialize(envelopeJson, topic);
        }
        catch
        {
            // A malformed envelope still counts as an observed delivery; record the raw body so tests can see it.
            context = new MessageContext
            {
                Topic = topic,
                Body = envelopeJson,
                Headers = new Dictionary<string, string>(),
            };
        }

        recorder.Record(new RecordedMessage(topic, context, disposition));
    }
}
