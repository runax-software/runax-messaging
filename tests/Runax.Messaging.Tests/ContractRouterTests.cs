using System.Collections.Frozen;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;

namespace Runax.Messaging.Tests;

public class ContractRouterTests
{
    private const string V1 = "https://contracts.test/orders/order-placed/v1";
    private const string V2 = "https://contracts.test/orders/order-placed/v2";
    private const string V3 = "https://contracts.test/orders/order-placed/v3";

    // --- Identified messages ---------------------------------------------------------------------

    [Fact]
    public void An_unversioned_consumer_accepts_everything()
    {
        var any = Consumer();

        Route([any], schema: V2, version: 2).Matched.ShouldBe([any]);
        Route([any]).Matched.ShouldBe([any]);
    }

    [Fact]
    public void A_schema_consumer_accepts_its_own_line_and_the_lines_it_reads()
    {
        var consumer = Consumer(version: 3, schema: V3, reads: [V2]);

        Route([consumer], schema: V3).Matched.ShouldBe([consumer]);
        Route([consumer], schema: V2).Matched.ShouldBe([consumer]);
        Route([consumer], schema: V1).Matched.ShouldBeEmpty();
    }

    [Fact]
    public void A_mismatched_schema_is_rejected_even_when_the_version_matches()
    {
        var consumer = Consumer(version: 2, schema: V2);

        var route = Route([consumer], schema: V1, version: 2);

        route.Matched.ShouldBeEmpty();
        route.Reason.ShouldBe(UnroutableReason.NoMatchingContract);
    }

    [Fact]
    public void The_safety_valve_delivers_a_mismatched_schema_by_a_matching_version()
    {
        var consumer = Consumer(version: 2, schema: V2);
        var settings = new BusContractSettings(RequireContractIdentity: false, SchemaMismatch.Report);

        var route = Route([consumer], schema: V1, version: 2, settings: settings);

        route.Matched.ShouldBe([consumer]);
        route.Mismatched.ShouldBe([consumer]);
        Route([consumer], schema: V1, version: 1, settings: settings).Matched.ShouldBeEmpty();
    }

    [Fact]
    public void Without_a_schema_on_the_wire_the_version_decides()
    {
        var consumer = Consumer(version: 2, schema: V2);

        Route([consumer], version: 2).Matched.ShouldBe([consumer]);
        Route([consumer], version: 3).Matched.ShouldBeEmpty();
    }

    [Fact]
    public void A_version_only_consumer_routes_by_the_version_and_ignores_the_schema()
    {
        var legacy = Consumer(version: 2);

        Route([legacy], schema: V2, version: 2).Matched.ShouldBe([legacy]);
        Route([legacy], schema: V2).Matched.ShouldBeEmpty();
    }

    [Fact]
    public void A_consumer_accepts_the_legacy_version_its_read_lines_record()
    {
        var consumer = Consumer(version: 3, schema: V3, reads: [V2]);
        var matcher = new FakeMatcher { LegacyVersions = { [V2] = 2, [V3] = 3 } };

        Route([consumer], version: 2, matcher: matcher).Matched.ShouldBe([consumer]);
        Route([consumer], version: 1, matcher: matcher).Matched.ShouldBeEmpty();
    }

    // --- Unidentified messages ---------------------------------------------------------------------

    [Fact]
    public void An_unidentified_message_is_routed_by_the_line_its_body_validates_against()
    {
        var v1 = Consumer(version: 1, schema: V1);
        var v2 = Consumer(version: 2, schema: V2);
        var matcher = new FakeMatcher { Accepted = [V2] };

        var route = Route([v1, v2], matcher: matcher);

        route.Matched.ShouldBe([v2]);
        route.Inferred.ShouldBe([V2]);
        matcher.Candidates.ShouldBe([V1, V2], ignoreOrder: true);
    }

    [Fact]
    public void An_unidentified_message_no_line_accepts_is_unroutable_with_the_failures()
    {
        var consumer = Consumer(version: 2, schema: V2);
        var matcher = new FakeMatcher { Failures = [$"{V2} at /currency: required"] };

        var route = Route([consumer], matcher: matcher);

        route.Matched.ShouldBeEmpty();
        route.Reason.ShouldBe(UnroutableReason.NoMatchingSchema);
        route.Detail.ShouldBe($"{V2} at /currency: required");
    }

    [Fact]
    public void An_ambiguous_message_reaches_no_schema_consumer_but_still_reaches_accept_all_consumers()
    {
        var v1 = Consumer(version: 1, schema: V1);
        var v2 = Consumer(version: 2, schema: V2);
        var audit = Consumer();
        var matcher = new FakeMatcher { Accepted = [V1, V2] };

        var route = Route([v1, v2, audit], matcher: matcher);

        route.Ambiguous.ShouldBeTrue();
        route.Inferred.ShouldBeEmpty();
        route.Matched.ShouldBe([audit]);
    }

    [Fact]
    public void An_ambiguous_message_with_no_accept_all_consumer_is_unroutable_as_ambiguous()
    {
        var matcher = new FakeMatcher { Accepted = [V1, V2] };

        var route = Route([Consumer(version: 1, schema: V1), Consumer(version: 2, schema: V2)], matcher: matcher);

        route.Matched.ShouldBeEmpty();
        route.Reason.ShouldBe(UnroutableReason.AmbiguousSchema);
        route.Detail.ShouldBe($"{V1}, {V2}");
    }

    [Fact]
    public void Lines_that_reach_the_same_consumers_are_not_ambiguous()
    {
        var consumer = Consumer(version: 3, schema: V3, reads: [V2]);
        var matcher = new FakeMatcher { Accepted = [V2, V3] };

        var route = Route([consumer], matcher: matcher);

        route.Ambiguous.ShouldBeFalse();
        route.Matched.ShouldBe([consumer]);
    }

    [Fact]
    public void A_bus_that_requires_identity_never_routes_by_validation()
    {
        var matcher = new FakeMatcher { Accepted = [V2] };
        var settings = new BusContractSettings(RequireContractIdentity: true, SchemaMismatch.Reject);

        var route = Route([Consumer(version: 2, schema: V2)], matcher: matcher, settings: settings);

        route.Matched.ShouldBeEmpty();
        route.Reason.ShouldBe(UnroutableReason.NoMatchingContract);
        matcher.Candidates.ShouldBeNull();
    }

    [Fact]
    public void Without_a_matcher_unidentified_messages_only_reach_unversioned_consumers()
    {
        var schemaConsumer = Consumer(version: 2, schema: V2);
        var any = Consumer();

        Route([schemaConsumer, any]).Matched.ShouldBe([any]);
    }

    [Fact]
    public void An_identified_message_is_never_routed_by_validation()
    {
        var matcher = new FakeMatcher { Accepted = [V2] };

        Route([Consumer(version: 2, schema: V2)], version: 9, matcher: matcher).Matched.ShouldBeEmpty();
        matcher.Candidates.ShouldBeNull();
    }

    // --- Helpers ---------------------------------------------------------------------------------

    private static ContractRoute Route(
        IReadOnlyList<IMessageConsumer> consumers,
        string? schema = null,
        int? version = null,
        FakeMatcher? matcher = null,
        BusContractSettings? settings = null) =>
        new ContractRouter(matcher, settings ?? BusContractSettings.Default).Route(consumers, new MessageContext
        {
            Topic = "orders",
            Body = """{"orderId":"1"}""",
            Headers = new Dictionary<string, string>(),
            ContractSchema = schema,
            ContractVersion = version,
        });

    private static FakeConsumer Consumer(int? version = null, string? schema = null, string[]? reads = null) =>
        new(version, schema, schema is null ? [] : [schema, .. reads ?? []]);

    private sealed class FakeConsumer(int? version, string? schema, string[] schemas) : IMessageConsumer
    {
        public string Topic => "orders";

        public int? ContractVersion => version;

        public string? ContractSchema => schema;

        public FrozenSet<string> ContractSchemas { get; } = schemas.ToFrozenSet(StringComparer.Ordinal);

        public ValueTask HandleAsync(MessageContext context, CancellationToken cancellationToken = default) =>
            ValueTask.CompletedTask;
    }

    private sealed class FakeMatcher : IContractSchemaMatcher
    {
        public Dictionary<string, int> LegacyVersions { get; } = [];

        public string[] Accepted { get; init; } = [];

        public string[] Failures { get; init; } = [];

        public IReadOnlyCollection<string>? Candidates { get; private set; }

        public int? LegacyVersionOf(string contractSchema) =>
            LegacyVersions.TryGetValue(contractSchema, out var version) ? version : null;

        public ContractMatch Match(string body, IReadOnlyCollection<string> candidates)
        {
            Candidates = candidates;
            return new ContractMatch(Accepted.Where(candidates.Contains).ToArray(), Failures);
        }
    }
}
