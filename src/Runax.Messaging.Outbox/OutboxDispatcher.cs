using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Outbox;

/// <summary>
/// Background service that periodically drains one bus's pending messages from its
/// <see cref="IOutboxStore"/> and publishes them to the bus's transport, marking each dispatched
/// on success. Each bus with an outbox runs its own dispatcher.
/// </summary>
internal sealed class OutboxDispatcher(
    string busName,
    IServiceProvider serviceProvider,
    IOptionsMonitor<OutboxOptions> optionsMonitor,
    ILogger<OutboxDispatcher> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = optionsMonitor.Get(busName);
        var store = serviceProvider.GetRequiredKeyedService<IOutboxStore>(busName);
        var transport = serviceProvider.GetRequiredKeyedService<IMessagingTransport>(busName);

        logger.LogInformation(
            "Outbox dispatcher for bus '{Bus}' started, polling every {Interval}.", busName, options.PollingInterval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var pending = await store.GetPendingAsync(busName, options.BatchSize, stoppingToken);

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
                logger.LogError(ex,
                    "Outbox dispatch for bus '{Bus}' failed; retrying after {Interval}.", busName, options.PollingInterval);
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

        logger.LogInformation("Outbox dispatcher for bus '{Bus}' shutting down.", busName);
    }
}
