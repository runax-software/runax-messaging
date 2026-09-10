namespace Runax.Messaging.Transports.Aws.Sqs.Tests;

public class SqsConfigTests
{
    [Fact]
    public void Defaults_are_sensible()
    {
        var config = new SqsConfig();

        config.Region.ShouldBe("us-east-1");
        config.AccessKey.ShouldBeNull();
        config.SecretKey.ShouldBeNull();
        config.ServiceUrl.ShouldBeNull();
        config.MaxNumberOfMessages.ShouldBe(10);
        config.WaitTimeSeconds.ShouldBe(20);
        config.TopicQueueUrlMap.ShouldBeEmpty();
    }
}
