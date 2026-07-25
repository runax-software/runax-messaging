namespace Runax.Messaging.Abstractions;

/// <summary>
/// Resolves an <see cref="IMessagePublisher"/> bound to a specific transport, so an application that
/// registers several transports can publish to each one explicitly (e.g. send an event to both Kafka
/// and SQS by publishing through <c>ForTransport("kafka")</c> and <c>ForTransport("sqs")</c>). The plain
/// <see cref="IMessagePublisher"/> keeps targeting a single transport (the sole one registered, or the
/// one chosen with <c>PublishTo</c>).
/// </summary>
public interface IMessagePublisherFactory
{
    /// <summary>
    /// Returns a publisher that always publishes to the transport whose
    /// <see cref="IMessagingTransport.SystemName"/> equals <paramref name="systemName"/>.
    /// </summary>
    /// <param name="systemName">The target transport's system name (e.g. <c>"kafka"</c>, <c>"sqs"</c>).</param>
    /// <returns>A publisher pinned to that transport.</returns>
    /// <exception cref="System.InvalidOperationException">
    /// No registered transport reports the given <paramref name="systemName"/>.
    /// </exception>
    IMessagePublisher ForTransport(string systemName);
}
