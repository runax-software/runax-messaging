using System.Text.Json;
using System.Text.Json.Serialization;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Tests;

public class SystemTextJsonSerializerTests
{
    public sealed record Order(int Id, string Name);

    public sealed record Unlisted(int Value);

    [Fact]
    public void Round_trips_through_a_source_generated_resolver()
    {
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions { TypeInfoResolver = SerializerTestJsonContext.Default });

        var json = serializer.Serialize(new Order(7, "widget"));

        json.ShouldBe("""{"Id":7,"Name":"widget"}""");
        serializer.Deserialize<Order>(json).ShouldBe(new Order(7, "widget"));
    }

    [Fact]
    public void A_configured_resolver_is_not_backed_by_reflection()
    {
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions { TypeInfoResolver = SerializerTestJsonContext.Default });

        Should.Throw<NotSupportedException>(() => serializer.Serialize(new Unlisted(1)));
    }

    [Fact]
    public void Options_without_a_resolver_fall_back_to_reflection()
    {
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        serializer.Serialize(new Unlisted(1)).ShouldBe("""{"value":1}""");
    }

    [Fact]
    public void The_callers_options_stay_mutable()
    {
        var options = new JsonSerializerOptions();
        var serializer = new SystemTextJsonSerializer(options);

        serializer.Serialize(new Order(1, "a"));

        options.IsReadOnly.ShouldBeFalse();
        options.TypeInfoResolver.ShouldBeNull();
    }

    [Fact]
    public void Read_only_options_are_used_as_is()
    {
        var options = new JsonSerializerOptions { TypeInfoResolver = SerializerTestJsonContext.Default };
        options.MakeReadOnly();

        new SystemTextJsonSerializer(options).Serialize(new Order(2, "b")).ShouldBe("""{"Id":2,"Name":"b"}""");
    }
}

[JsonSerializable(typeof(SystemTextJsonSerializerTests.Order))]
internal sealed partial class SerializerTestJsonContext : JsonSerializerContext;
