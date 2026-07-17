using Runax.Messaging.Sqs;

namespace Runax.Messaging.Sqs.Tests;

public class SqsOptionsTests
{
    [Fact]
    public void Defaults_are_sensible()
    {
        var options = new SqsOptions();

        options.Region.ShouldBe("us-east-1");
        options.AccessKey.ShouldBeNull();
        options.SecretKey.ShouldBeNull();
        options.ServiceUrl.ShouldBeNull();
        options.MaxNumberOfMessages.ShouldBe(10);
        options.WaitTimeSeconds.ShouldBe(20);
        options.TopicQueueUrlMap.ShouldBeEmpty();
    }
}
