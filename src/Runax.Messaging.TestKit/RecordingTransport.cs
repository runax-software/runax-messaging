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
    RetryOptions retryOptions,
    BusHeaderSettings headerSettings,
    string busName) : IMessagingTransport
{
    public string SystemName => inner.SystemName;

    public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default) =>
        PublishAsync(new TransportMessage(topic, envelopeJson), cancellationToken);

    public ValueTask PublishAsync(TransportMessage message, CancellationToken cancellationToken = default)
    {
        RecordDeadLetterPublish(message);
        return inner.PublishAsync(message, cancellationToken);
    }

    public ValueTask PublishBatchAsync(
        string topic,
        IReadOnlyList<string> envelopeJsons,
        CancellationToken cancellationToken = default) =>
        PublishBatchAsync(envelopeJsons.Select(e => new TransportMessage(topic, e)).ToList(), cancellationToken);

    public async ValueTask PublishBatchAsync(
        IReadOnlyList<TransportMessage> messages,
        CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
            RecordDeadLetterPublish(message);

        await inner.PublishBatchAsync(messages, cancellationToken).ConfigureAwait(false);
    }

    public Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default) =>
        SubscribeAsync(topics, message => onMessage(message.Body, message.Topic), cancellationToken);

    public Task SubscribeAsync(
        string[] topics,
        Func<TransportMessage, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default) =>
        inner.SubscribeAsync(topics, async message =>
        {
            var disposition = await onMessage(message).ConfigureAwait(false);
            Record(message, disposition);
            return disposition;
        }, cancellationToken);

    private void RecordDeadLetterPublish(TransportMessage message)
    {
        // Dead-letter topics are never subscribed, so their messages never reach the delivery path above.
        // Record them here so WaitForDeadLetterAsync can observe them. A normal publish is recorded when it is
        // consumed, so this only ever fires for the framework-managed dead-letter topic.
        if (retryOptions.DeadLetterTopicSuffix.Length > 0
            && message.Topic.EndsWith(retryOptions.DeadLetterTopicSuffix, StringComparison.Ordinal))
        {
            Record(message, MessageDisposition.DeadLetter);
        }
    }

    private void Record(TransportMessage message, MessageDisposition disposition)
    {
        MessageContext context;
        try
        {
            context = NativeHeaders.Merge(
                serializer.Deserialize(message.Body, message.Topic), message.Headers, headerSettings.Precedence);
        }
        catch
        {
            // A malformed envelope still counts as an observed delivery; record the raw body so tests can see it.
            context = new MessageContext
            {
                Topic = message.Topic,
                Body = message.Body,
                Headers = new Dictionary<string, string>(message.Headers),
            };
        }

        recorder.Record(new RecordedMessage(busName, message.Topic, context, disposition));
    }
}
