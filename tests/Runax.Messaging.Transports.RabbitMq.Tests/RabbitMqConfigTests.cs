namespace Runax.Messaging.Transports.RabbitMq.Tests;

public class RabbitMqConfigTests
{
    [Fact]
    public void Defaults_are_sensible()
    {
        var config = new RabbitMqConfig();

        config.HostName.ShouldBe("localhost");
        config.Port.ShouldBe(5672);
        config.UserName.ShouldBe("guest");
        config.Password.ShouldBe("guest");
        config.VirtualHost.ShouldBe("/");
        config.ExchangeName.ShouldBe("runax.messaging");
        config.ExchangeType.ShouldBe("topic");
    }
}
