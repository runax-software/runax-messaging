namespace Runax.Messaging.RabbitMq;

/// <summary>
/// Configuration options for the RabbitMQ messaging transport.
/// </summary>
public sealed class RabbitMqOptions
{
    /// <summary>
    /// Gets or sets the RabbitMQ host name. Defaults to "localhost".
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the RabbitMQ port. Defaults to 5672.
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// Gets or sets the RabbitMQ username. Defaults to "guest".
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the RabbitMQ password. Defaults to "guest".
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the RabbitMQ virtual host. Defaults to "/".
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Gets or sets the exchange name. Defaults to "runax.messaging".
    /// </summary>
    public string ExchangeName { get; set; } = "runax.messaging";

    /// <summary>
    /// Gets or sets the exchange type. Defaults to "topic".
    /// </summary>
    public string ExchangeType { get; set; } = "topic";

    /// <summary>
    /// Gets or sets the consumer prefetch count (unacknowledged messages allowed in flight). Defaults to 10.
    /// </summary>
    public ushort PrefetchCount { get; set; } = 10;

    /// <summary>
    /// Gets or sets a value indicating whether publisher confirms are enabled so that
    /// <see cref="RabbitMqTransport"/> waits for broker acknowledgement of each publish. Defaults to <see langword="true"/>.
    /// </summary>
    public bool PublisherConfirms { get; set; } = true;

    /// <summary>
    /// Gets or sets how long to wait for a publisher confirm before failing the publish. Defaults to 5 seconds.
    /// </summary>
    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the number of channels kept in the publish channel pool. Because <c>IModel</c> is not
    /// thread-safe, publishes are serialized per channel; a larger pool allows more concurrent publishes. Defaults to 5.
    /// </summary>
    public int PublishChannelPoolSize { get; set; } = 5;

    /// <summary>
    /// Gets or sets the dead-letter exchange that consumer queues route rejected messages to when the
    /// dispatch pipeline uses <c>DeadLetterStrategy.BrokerNative</c>. When set, subscriber queues are declared
    /// with <c>x-dead-letter-exchange</c> and the exchange is declared as durable. When <see langword="null"/>,
    /// rejected messages are dropped by the broker. Defaults to <see langword="null"/>.
    /// </summary>
    public string? DeadLetterExchange { get; set; }

    /// <summary>
    /// Gets or sets the type of the <see cref="DeadLetterExchange"/> declared for broker-native dead-lettering. Defaults to "topic".
    /// </summary>
    public string DeadLetterExchangeType { get; set; } = "topic";
}
