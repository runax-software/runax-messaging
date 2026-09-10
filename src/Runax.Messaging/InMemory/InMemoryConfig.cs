using Runax.Messaging.Abstractions;

namespace Runax.Messaging.InMemory;

/// <summary>
/// Transport config for the built-in in-process transport. Messages are delivered within the same
/// process; useful for tests and single-process scenarios:
/// <c>bus.AddTransport(new InMemoryConfig())</c>.
/// </summary>
public sealed class InMemoryConfig : TransportConfig
{
    /// <inheritdoc />
    public override string SystemName => InMemoryTransport.TransportName;

    /// <inheritdoc />
    protected internal override IMessagingTransport CreateTransport(TransportContext context) =>
        new InMemoryTransport();
}
