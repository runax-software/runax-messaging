using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.TestKit;

/// <summary>
/// Transport config the harness registers on every harness bus: an in-memory transport wrapped in
/// a <see cref="RecordingTransport"/> so the shared <see cref="MessageRecorder"/> observes every
/// delivery and dead-letter publish on that bus.
/// </summary>
internal sealed class RecordingTransportConfig : TransportConfig
{
    public override string SystemName => "in-memory";

    protected override IMessagingTransport CreateTransport(TransportContext context) =>
        new RecordingTransport(
            new InMemoryTransport(),
            context.Services.GetRequiredService<MessageRecorder>(),
            context.Services.GetRequiredService<IMessageSerializer>(),
            context.Services.GetRequiredService<RetryOptions>(),
            context.BusName);
}
