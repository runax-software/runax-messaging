using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Runax.Messaging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Outbox;

/// <summary>
/// Background service that periodically drains pending messages from the <see cref="IOutboxStore"/>
/// and publishes them to the transport, marking each dispatched on success. When several transports
/// are registered it publishes to the one selected with <c>PublishTo</c>, matching the publisher.
/// </summary>
internal sealed class OutboxDispatcher(
    IOutboxStore store,
    IEnumerable<IMessagingTransport> transports,
    MessagingPublishOptions publishOptions,
    OutboxOptions options,
    ILogger<OutboxDispatcher> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var transport = PublishTargetSelector.Select(
            transports as IReadOnlyList<IMessagingTransport> ?? transports.ToArray(),
            publishOptions.DefaultTransport);

        logger.LogInformation("Outbox dispatcher started, polling every {Interval}.", options.PollingInterval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var pending = await store.GetPendingAsync(options.BatchSize, stoppingToken);

                foreach (var message in pending)
                {
                    if (stoppingToken.IsCancellationRequested) break;

                    await transport.PublishAsync(message.Topic, message.Payload, stoppingToken);
                    await store.MarkDispatchedAsync(message.Id, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // A failed publish leaves the message pending; it is retried on the next poll.
                logger.LogError(ex, "Outbox dispatch failed; retrying after {Interval}.", options.PollingInterval);
            }

            try
            {
                await Task.Delay(options.PollingInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        logger.LogInformation("Outbox dispatcher shutting down.");
    }
}
