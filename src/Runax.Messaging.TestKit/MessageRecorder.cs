using System.Collections.Concurrent;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.TestKit;

/// <summary>
/// Thread-safe sink that collects every message the harness observes and lets tests await deliveries.
/// Registered as a singleton so the recording transport and the harness share one instance.
/// </summary>
internal sealed class MessageRecorder
{
    private readonly ConcurrentQueue<RecordedMessage> _messages = new();
    private readonly object _gate = new();
    private readonly List<(Func<RecordedMessage, bool> Predicate, TaskCompletionSource<RecordedMessage> Signal)> _waiters = [];

    public IReadOnlyList<RecordedMessage> Messages => _messages.ToArray();

    public void Record(RecordedMessage message)
    {
        _messages.Enqueue(message);

        List<TaskCompletionSource<RecordedMessage>>? completed = null;
        lock (_gate)
        {
            for (var i = _waiters.Count - 1; i >= 0; i--)
            {
                if (!_waiters[i].Predicate(message))
                    continue;

                (completed ??= []).Add(_waiters[i].Signal);
                _waiters.RemoveAt(i);
            }
        }

        if (completed is null)
            return;

        foreach (var signal in completed)
            signal.TrySetResult(message);
    }

    public Task<RecordedMessage> WaitAsync(Func<RecordedMessage, bool> predicate, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<RecordedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        lock (_gate)
        {
            // Satisfy from already-observed messages first so a wait registered after delivery still completes.
            foreach (var recorded in _messages)
            {
                if (!predicate(recorded))
                    continue;

                tcs.SetResult(recorded);
                return tcs.Task;
            }

            _waiters.Add((predicate, tcs));
        }

        return cancellationToken.CanBeCanceled
            ? tcs.Task.WaitAsync(cancellationToken)
            : tcs.Task;
    }
}
