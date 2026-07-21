using System.Collections.Concurrent;

namespace Runax.Messaging.Outbox;

/// <summary>
/// In-process <see cref="IOutboxStore"/> for tests and single-process scenarios. It is not durable
/// and its <see cref="AddAsync"/> commits immediately, so it does not provide the transactional
/// atomicity of a database-backed store — use it as a reference implementation only.
/// </summary>
public sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly ConcurrentDictionary<Guid, OutboxMessage> _messages = new();

    /// <inheritdoc />
    public Task AddAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        _messages[message.Id] = message;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int maxCount, CancellationToken cancellationToken = default)
    {
        IReadOnlyList<OutboxMessage> pending = _messages.Values
            .Where(m => m.DispatchedAt is null)
            .OrderBy(m => m.CreatedAt)
            .Take(maxCount)
            .ToList();

        return Task.FromResult(pending);
    }

    /// <inheritdoc />
    public Task MarkDispatchedAsync(Guid id, CancellationToken cancellationToken = default)
    {
        if (_messages.TryGetValue(id, out var message))
            message.DispatchedAt = DateTimeOffset.UtcNow;

        return Task.CompletedTask;
    }
}
