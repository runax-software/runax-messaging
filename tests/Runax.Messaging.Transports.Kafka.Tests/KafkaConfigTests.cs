namespace Runax.Messaging.Transports.Kafka.Tests;

public class KafkaConfigTests
{
    [Fact]
    public void Defaults_are_sensible()
    {
        var config = new KafkaConfig();

        config.BootstrapServers.ShouldBe(string.Empty);
        config.ConsumerGroupId.ShouldBe("runax");
        config.AutoOffsetReset.ShouldBe("earliest");
        config.Acks.ShouldBe("all");
        config.EnableIdempotence.ShouldBeTrue();
        config.DeadLetterTopicSuffix.ShouldBe(".dead-letter");
    }
}
