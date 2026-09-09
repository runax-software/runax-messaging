using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Tests;

/// <summary>
/// Wraps a pre-built transport instance so tests can attach fakes and recording transports to a
/// bus via <c>bus.AddTransport(new FakeTransportConfig(transport))</c>.
/// </summary>
internal sealed class FakeTransportConfig(IMessagingTransport transport) : TransportConfig
{
    public override string SystemName => transport.SystemName;

    protected internal override IMessagingTransport CreateTransport(TransportContext context) => transport;
}
