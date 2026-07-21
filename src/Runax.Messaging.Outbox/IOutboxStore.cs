namespace Runax.Messaging.Outbox;

/// <summary>
/// Persistence for outbox messages. Implement this over your database so that
/// <see cref="AddAsync"/> participates in the same transaction as your business data —
/// that atomic write is what makes the pattern reliable.
/// </summary>
public interface IOutboxStore
{
    /// <summary>
    /// Persists a message to the outbox. Implementations should enlist this write in the caller's
    /// current unit of work / transaction rather than committing on their own.
    /// </summary>
    /// <param name="message">The message to persist.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task AddAsync(OutboxMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns up to <paramref name="maxCount"/> pending (not yet dispatched) messages, oldest first.
    /// </summary>
    /// <param name="maxCount">The maximum number of messages to return.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int maxCount, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks a message as dispatched so it is not published again.
    /// </summary>
    /// <param name="id">The identifier of the dispatched message.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task MarkDispatchedAsync(Guid id, CancellationToken cancellationToken = default);
}
