using Runax.Messaging.Transports.Kafka;

namespace Runax.Messaging.Transports.Kafka.Tests;

public class KafkaOptionsTests
{
    [Fact]
    public void Defaults_are_sensible()
    {
        var options = new KafkaOptions();

        options.BootstrapServers.ShouldBe(string.Empty);
        options.ConsumerGroupId.ShouldBe("runax");
        options.AutoOffsetReset.ShouldBe("earliest");
        options.Acks.ShouldBe("all");
        options.EnableIdempotence.ShouldBeTrue();
        options.DeadLetterTopicSuffix.ShouldBe(".dead-letter");
    }
}
