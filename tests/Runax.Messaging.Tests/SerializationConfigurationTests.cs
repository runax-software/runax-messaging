using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.InMemory;
using Runax.Messaging.Serialization;

namespace Runax.Messaging.Tests;

public class SerializationConfigurationTests
{
    private sealed record Thing(int Value);

    [Fact]
    public void ConfigureSerialization_applies_options_to_body_serialize_and_deserialize()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m
            .AddInMemory()
            .ConfigureSerialization(o => o.PropertyNamingPolicy = JsonNamingPolicy.CamelCase));
        using var provider = services.BuildServiceProvider();

        var serializer = provider.GetRequiredService<IMessageSerializer>();

        var envelope = serializer.Serialize(new Thing(42), headers: null);
        var context = serializer.Deserialize(envelope, "things");

        // Body is camelCase (options applied on serialize)...
        context.Body.ShouldContain("\"value\"");
        // ...and the same options are honored on deserialize (would be 0 with default case-sensitive options).
        context.Deserialize<Thing>()!.Value.ShouldBe(42);
    }

    [Fact]
    public void Default_serializer_round_trips_without_configuration()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRunaxMessaging(m => m.AddInMemory());
        using var provider = services.BuildServiceProvider();

        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var context = serializer.Deserialize(serializer.Serialize(new Thing(7), null), "things");

        context.Deserialize<Thing>()!.Value.ShouldBe(7);
    }
}
