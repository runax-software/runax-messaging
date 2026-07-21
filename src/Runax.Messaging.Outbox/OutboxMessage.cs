namespace Runax.Messaging.Outbox;

/// <summary>
/// A message persisted in the outbox awaiting dispatch to the transport.
/// </summary>
public sealed record OutboxMessage
{
    /// <summary>
    /// Gets the unique identifier of the outbox entry.
    /// </summary>
    public Guid Id { get; init; } = Guid.NewGuid();

    /// <summary>
    /// Gets the topic the message will be published to.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the serialized message envelope to publish.
    /// </summary>
    public required string Payload { get; init; }

    /// <summary>
    /// Gets the time the message was enqueued.
    /// </summary>
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Gets or sets the time the message was dispatched, or <see langword="null"/> while it is still pending.
    /// </summary>
    public DateTimeOffset? DispatchedAt { get; set; }
}
