using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Consumers;

namespace Runax.Messaging;

/// <summary>
/// A topic and the contract version a registered consumer handles (<see langword="null"/> = unversioned).
/// </summary>
public sealed record HandledContract(string Topic, int? Version);

/// <summary>
/// Introspects which topics and contract versions this application consumes, so you can verify version
/// coverage at startup (e.g. before letting a producer emit a new version).
/// </summary>
public interface IMessageContractCatalog
{
    /// <summary>
    /// Gets the distinct (topic, version) pairs handled by registered consumers.
    /// </summary>
    IReadOnlyCollection<HandledContract> Handled { get; }

    /// <summary>
    /// Returns whether a consumer would receive a message of <paramref name="version"/> on
    /// <paramref name="topic"/> — true if a matching versioned consumer or an unversioned (accept-all)
    /// consumer is registered for that topic.
    /// </summary>
    bool Accepts(string topic, int version);
}

internal sealed class MessageContractCatalog(
    IServiceProvider serviceProvider,
    IEnumerable<ConsumerRegistration> registrations) : IMessageContractCatalog
{
    private readonly Lazy<IReadOnlyCollection<HandledContract>> _handled =
        new(() => Build(serviceProvider, registrations));

    public IReadOnlyCollection<HandledContract> Handled => _handled.Value;

    public bool Accepts(string topic, int version) =>
        Handled.Any(h => h.Topic == topic && (h.Version is null || h.Version == version));

    private static HandledContract[] Build(
        IServiceProvider serviceProvider,
        IEnumerable<ConsumerRegistration> registrations)
    {
        var handled = new HashSet<HandledContract>();
        foreach (var registration in registrations)
        {
            var consumer = (IMessageConsumer)serviceProvider.GetRequiredService(registration.ConsumerType);
            handled.Add(new HandledContract(consumer.Topic, consumer.ContractVersion));
        }

        return handled.ToArray();
    }
}
