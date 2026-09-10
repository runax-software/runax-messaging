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
    public void Bus_ConfigureSerialization_applies_only_to_that_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
        }));
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();

        // The default bus got camelCase...
        serializers.For(BusNames.Default, "any").Serialize(new Thing(42), null).ShouldContain("\"value\"");
        // ...but an unconfigured bus still uses the global (PascalCase) default.
        serializers.For("other", "any").Serialize(new Thing(42), null).ShouldContain("\"Value\"");
    }

    [Fact]
    public void Bus_UseSerializer_replaces_the_body_only_for_that_bus()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.UseSerializer<MarkerSerializer>();
        }));
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();

        var scoped = serializers.For(BusNames.Default, "any").Serialize(new Thing(1), null);
        scoped.ShouldContain("\"marker\":true");                // the custom body serializer ran
        scoped.ShouldContain(EnvelopeSerializer.MetadataKey);   // the envelope is still framework-owned

        // A different bus keeps the global serializer (PascalCase, no marker).
        serializers.For("other", "any").Serialize(new Thing(1), null).ShouldContain("\"Value\"");
    }

    [Fact]
    public void UseSerializerForTopic_applies_only_to_that_topic()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.UseSerializerForTopic<MarkerSerializer>("orders");
        }));
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();

        // The "orders" topic uses the custom body serializer on this bus...
        serializers.For(BusNames.Default, "orders").Serialize(new Thing(1), null).ShouldContain("\"marker\":true");
        // ...while every other topic keeps the global (PascalCase) default.
        serializers.For(BusNames.Default, "shipments").Serialize(new Thing(1), null).ShouldContain("\"Value\"");
    }

    [Fact]
    public void Topic_serializer_wins_over_bus_serializer_for_the_same_topic()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddBus(bus =>
        {
            bus.AddTransport(new InMemoryConfig());
            bus.ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase);
            bus.UseSerializerForTopic<MarkerSerializer>("orders");
        }));
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();

        // "orders" gets the topic serializer (marker), not the bus's camelCase serializer.
        serializers.For(BusNames.Default, "orders").Serialize(new Thing(1), null).ShouldContain("\"marker\":true");
        // Another topic on the same bus still gets the bus-wide camelCase serializer.
        serializers.For(BusNames.Default, "shipments").Serialize(new Thing(1), null).ShouldContain("\"value\"");
    }

    [Fact]
    public void Each_bus_keeps_its_own_topic_serializer_for_the_same_topic()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus(bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.UseSerializerForTopic<MarkerSerializer>("orders");
            });
            m.AddBus("other", bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.UseSerializerForTopic<OtherMarkerSerializer>("orders");
            });
        });
        using var provider = services.BuildServiceProvider();

        var serializers = provider.GetRequiredService<IMessageSerializerProvider>();

        // On the default bus its topic serializer applies...
        serializers.For(BusNames.Default, "orders").Serialize(new Thing(1), null).ShouldContain("\"marker\":true");
        // ...while the other bus uses its own topic serializer for the same topic.
        serializers.For("other", "orders").Serialize(new Thing(1), null).ShouldContain("\"otherMarker\":true");
    }

    private sealed class MarkerSerializer : ISerializer
    {
        public string Serialize<TMessage>(TMessage message) => """{"marker":true}""";

        public TMessage? Deserialize<TMessage>(string body) => default;
    }

    private sealed class OtherMarkerSerializer : ISerializer
    {
        public string Serialize<TMessage>(TMessage message) => """{"otherMarker":true}""";

        public TMessage? Deserialize<TMessage>(string body) => default;
    }
}
