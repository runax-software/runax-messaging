using Runax.Messaging.RabbitMq;

namespace Runax.Messaging.RabbitMq.Tests;

public class RabbitMqOptionsTests
{
    [Fact]
    public void Defaults_are_sensible()
    {
        var options = new RabbitMqOptions();

        options.HostName.ShouldBe("localhost");
        options.Port.ShouldBe(5672);
        options.UserName.ShouldBe("guest");
        options.Password.ShouldBe("guest");
        options.VirtualHost.ShouldBe("/");
        options.ExchangeName.ShouldBe("runax.messaging");
        options.ExchangeType.ShouldBe("topic");
    }
}
