using System.ComponentModel.DataAnnotations;

namespace Runax.Messaging.Transports.RabbitMq;

/// <summary>
/// Configuration options for the RabbitMQ messaging transport.
/// </summary>
public sealed class RabbitMqOptions
{
    /// <summary>
    /// Gets or sets the RabbitMQ host name. Defaults to "localhost". Ignored when <see cref="Uri"/> is set.
    /// </summary>
    [Required]
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the RabbitMQ port. Defaults to 5672. Ignored when <see cref="Uri"/> is set.
    /// </summary>
    [Range(1, 65535)]
    public int Port { get; set; } = 5672;

    /// <summary>
    /// Gets or sets the RabbitMQ username. Defaults to "guest". Ignored when <see cref="Uri"/> is set.
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the RabbitMQ password. Defaults to "guest". Ignored when <see cref="Uri"/> is set.
    /// Prefer <see cref="Uri"/> (an <c>amqps://</c> connection string) or a secret store over a plaintext value.
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// Gets or sets the RabbitMQ virtual host. Defaults to "/". Ignored when <see cref="Uri"/> is set.
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Gets or sets a full AMQP connection URI (e.g. <c>amqps://user:pass@host:5671/vhost</c>). When set, it takes
    /// precedence over <see cref="HostName"/>/<see cref="Port"/>/<see cref="UserName"/>/<see cref="Password"/>/<see cref="VirtualHost"/>
    /// and enables TLS automatically for the <c>amqps</c> scheme. Defaults to <see langword="null"/>.
    /// </summary>
    public string? Uri { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether TLS is enabled for the connection when connecting via the discrete
    /// host settings (not <see cref="Uri"/>). Defaults to <see langword="false"/>.
    /// </summary>
    public bool UseTls { get; set; }

    /// <summary>
    /// Gets or sets the TLS server name (SNI) used when <see cref="UseTls"/> is enabled.
    /// Defaults to <see cref="HostName"/> when <see langword="null"/>.
    /// </summary>
    public string? SslServerName { get; set; }

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
    [Range(1, int.MaxValue)]
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
