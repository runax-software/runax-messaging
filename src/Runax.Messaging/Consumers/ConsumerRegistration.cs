using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Consumers;

/// <summary>
/// Tracks metadata about a registered consumer for the hosting infrastructure.
/// </summary>
internal sealed class ConsumerRegistration
{
    /// <summary>
    /// Gets the CLR type of the consumer class. For a delegate consumer this is
    /// <see cref="DelegateConsumer"/>, and <see cref="Instance"/> holds the consumer itself.
    /// </summary>
    public required Type ConsumerType { get; init; }

    /// <summary>
    /// Gets the consumer instance when it was registered as one (a delegate consumer), or
    /// <see langword="null"/> when it is resolved from the container by <see cref="ConsumerType"/>.
    /// </summary>
    public IMessageConsumer? Instance { get; init; }

    /// <summary>
    /// Gets the name of the bus this consumer subscribes on. The same consumer type may be
    /// registered on several buses (one registration per bus).
    /// </summary>
    public required string Bus { get; init; }

    /// <summary>
    /// Gets how errors name this consumer: by its type, or by its topic for a delegate consumer.
    /// </summary>
    public string Description => Instance is null
        ? $"consumer '{ConsumerType.Name}'"
        : $"a delegate consumer on topic '{Instance.Topic}'";

    /// <summary>
    /// Returns the consumer: <see cref="Instance"/> when set, otherwise the container's
    /// <see cref="ConsumerType"/> singleton.
    /// </summary>
    /// <param name="serviceProvider">The application services.</param>
    /// <returns>The consumer to dispatch to.</returns>
    public IMessageConsumer Resolve(IServiceProvider serviceProvider) =>
        Instance ?? (IMessageConsumer)serviceProvider.GetRequiredService(ConsumerType);

    /// <summary>
    /// Creates the registration for a delegate consumer.
    /// </summary>
    /// <param name="bus">The bus to consume on.</param>
    /// <param name="topic">The topic to consume.</param>
    /// <param name="handler">Handles each message.</param>
    /// <returns>The registration.</returns>
    /// <exception cref="ArgumentException"><paramref name="topic"/> is null or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="handler"/> is null.</exception>
    public static ConsumerRegistration ForDelegate(
        string bus,
        string topic,
        Func<MessageContext, CancellationToken, ValueTask> handler)
    {
        ArgumentException.ThrowIfNullOrEmpty(topic);
        ArgumentNullException.ThrowIfNull(handler);

        return new ConsumerRegistration
        {
            ConsumerType = typeof(DelegateConsumer),
            Instance = new DelegateConsumer(topic, handler),
            Bus = bus,
        };
    }
}
