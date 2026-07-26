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
        serializers.For(transportName, "any").Serialize(new Thing(42), null).ShouldContain("\"value\"");
        // ...but an unconfigured broker still uses the global (PascalCase) default.
        serializers.For("other", "any").Serialize(new Thing(42), null).ShouldContain("\"Value\"");
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

        var scoped = serializers.For(transportName, "any").Serialize(new Thing(1), null);
        scoped.ShouldContain("\"marker\":true");                // the custom body serializer ran
        scoped.ShouldContain(EnvelopeSerializer.MetadataKey);   // the envelope is still framework-owned

        // A different broker keeps the global serializer (PascalCase, no marker).
        serializers.For("other", "any").Serialize(new Thing(1), null).ShouldContain("\"Value\"");
    }

    [Fact]
    public void UseSerializerForTopic_applies_only_to_that_topic()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory().UseSerializerForTopic<MarkerSerializer>("orders"));
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();
        var transportName = provider.GetRequiredService<IMessagingTransport>().SystemName;

        // The "orders" topic uses the custom body serializer on any transport...
        serializers.For(transportName, "orders").Serialize(new Thing(1), null).ShouldContain("\"marker\":true");
        // ...while every other topic keeps the global (PascalCase) default.
        serializers.For(transportName, "shipments").Serialize(new Thing(1), null).ShouldContain("\"Value\"");
    }

    [Fact]
    public void Topic_serializer_wins_over_broker_serializer_for_the_same_topic()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory(inMemory =>
        {
            inMemory.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
            inMemory.UseSerializerForTopic<MarkerSerializer>("orders");
        }));
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();
        var transportName = provider.GetRequiredService<IMessagingTransport>().SystemName;

        // "orders" gets the topic serializer (marker), not the broker's camelCase serializer.
        serializers.For(transportName, "orders").Serialize(new Thing(1), null).ShouldContain("\"marker\":true");
        // Another topic on the same broker still gets the per-broker camelCase serializer.
        serializers.For(transportName, "shipments").Serialize(new Thing(1), null).ShouldContain("\"value\"");
    }

    [Fact]
    public void Transport_scoped_topic_serializer_wins_over_global_topic_serializer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m
            .UseSerializerForTopic<GlobalMarkerSerializer>("orders")
            .AddInMemory(inMemory => inMemory.UseSerializerForTopic<MarkerSerializer>("orders")));
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();
        var transportName = provider.GetRequiredService<IMessagingTransport>().SystemName;

        // On the in-memory transport the transport-scoped topic serializer wins...
        serializers.For(transportName, "orders").Serialize(new Thing(1), null).ShouldContain("\"marker\":true");
        // ...but on any other transport the global per-topic serializer applies.
        serializers.For("other", "orders").Serialize(new Thing(1), null).ShouldContain("\"globalMarker\":true");
    }

    private sealed class MarkerSerializer : ISerializer
    {
        public string Serialize<TMessage>(TMessage message) => """{"marker":true}""";

        public TMessage? Deserialize<TMessage>(string body) => default;
    }

    private sealed class GlobalMarkerSerializer : ISerializer
    {
        public string Serialize<TMessage>(TMessage message) => """{"globalMarker":true}""";

        public TMessage? Deserialize<TMessage>(string body) => default;
    }
}
