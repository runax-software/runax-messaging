using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class ScopedSerializationTests
{
    private sealed record Thing(int Value);

    [Fact]
    public void Scoped_ConfigureSerialization_applies_only_to_that_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory(inMemory =>
            inMemory.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase)));
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();
        var transportName = provider.GetRequiredService<IMessagingTransport>().SystemName;

        // The in-memory broker got camelCase...
        serializers.For(transportName).Serialize(new Thing(42), null).ShouldContain("\"value\"");
        // ...but an unconfigured broker still uses the global (PascalCase) default.
        serializers.For("other").Serialize(new Thing(42), null).ShouldContain("\"Value\"");
    }

    [Fact]
    public void Scoped_UseSerializer_replaces_the_body_only_for_that_transport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory(inMemory => inMemory.UseSerializer<MarkerSerializer>()));
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();
        var transportName = provider.GetRequiredService<IMessagingTransport>().SystemName;

        var scoped = serializers.For(transportName).Serialize(new Thing(1), null);
        scoped.ShouldContain("\"marker\":true");                // the custom body serializer ran
        scoped.ShouldContain(EnvelopeSerializer.MetadataKey);   // the envelope is still framework-owned

        // A different broker keeps the global serializer (PascalCase, no marker).
        serializers.For("other").Serialize(new Thing(1), null).ShouldContain("\"Value\"");
    }

    private sealed class MarkerSerializer : ISerializer
    {
        public string Serialize<TMessage>(TMessage message) => """{"marker":true}""";

        public TMessage? Deserialize<TMessage>(string body) => default;
    }
}
