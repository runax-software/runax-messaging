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
}
