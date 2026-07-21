using System.ComponentModel.DataAnnotations;

namespace Runax.Messaging.Outbox;

/// <summary>
/// Options for the outbox dispatcher.
/// </summary>
public sealed class OutboxOptions
{
    /// <summary>
    /// Gets or sets how often the dispatcher polls the store for pending messages. Defaults to 5 seconds.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the maximum number of pending messages drained per poll. Defaults to 100.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int BatchSize { get; set; } = 100;
}
