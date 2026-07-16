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
}
