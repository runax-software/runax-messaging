using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;
using Runax.Messaging.InMemory;

namespace Runax.Messaging.Tests;

public class BusConfigurationTests
{
    private sealed class ValidatedConfig : TransportConfig
    {
        [Required]
        public string? Endpoint { get; set; }

        public override string SystemName => "validated";

        protected internal override IMessagingTransport CreateTransport(TransportContext context) =>
            new InMemoryTransport();
    }

    private static ServiceCollection NewServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        return services;
    }

    [Fact]
    public void A_second_AddTransport_on_the_same_bus_throws_at_configuration_time()
    {
        var services = NewServices();

        var ex = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
            {
                bus.AddTransport(new InMemoryConfig());
                bus.AddTransport(new InMemoryConfig());
            })));

        ex.Message.ShouldContain("already has a transport");
        ex.Message.ShouldContain("exactly one transport");
    }

    [Fact]
    public void A_bus_with_no_transport_throws_when_the_AddBus_block_completes()
    {
        var services = NewServices();

        var ex = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(_ => { })));

        ex.Message.ShouldContain("has no transport");
    }

    [Fact]
    public void A_duplicate_bus_name_throws_at_configuration_time()
    {
        var services = NewServices();

        var ex = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m =>
            {
                m.AddBus("events", bus => bus.AddTransport(new InMemoryConfig()));
                m.AddBus("events", bus => bus.AddTransport(new InMemoryConfig()));
            }));

        ex.Message.ShouldContain("'events'");
        ex.Message.ShouldContain("already registered");
    }

    [Fact]
    public void An_invalid_transport_config_fails_DataAnnotations_validation_at_configuration_time()
    {
        var services = NewServices();

        var ex = Should.Throw<InvalidOperationException>(() =>
            services.AddRunaxMessaging(m => m.AddBus(bus =>
                bus.AddTransport(new ValidatedConfig { Endpoint = null }))));

        ex.Message.ShouldContain("transport config is invalid");
        ex.Message.ShouldContain(nameof(ValidatedConfig));
    }

    [Fact]
    public void The_unkeyed_IBus_resolves_to_the_default_bus()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus(bus => bus.AddTransport(new InMemoryConfig()));
            m.AddBus("audit", bus => bus.AddTransport(new InMemoryConfig()));
        });

        using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IBus>().Name.ShouldBe(BusNames.Default);
    }

    [Fact]
    public void The_unkeyed_IBus_resolves_to_the_sole_named_bus_when_no_default_exists()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m =>
            m.AddBus("only", bus => bus.AddTransport(new InMemoryConfig())));

        using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IBus>().Name.ShouldBe("only");
    }

    [Fact]
    public void The_unkeyed_IBus_throws_when_several_named_buses_exist_and_none_is_default()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus("one", bus => bus.AddTransport(new InMemoryConfig()));
            m.AddBus("two", bus => bus.AddTransport(new InMemoryConfig()));
        });

        using var provider = services.BuildServiceProvider();
        var ex = Should.Throw<InvalidOperationException>(() => provider.GetRequiredService<IBus>());
        ex.Message.ShouldContain("ambiguous");
        ex.Message.ShouldContain("one");
        ex.Message.ShouldContain("two");
    }

    [Fact]
    public void Named_buses_resolve_via_keyed_services_and_the_provider()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus(bus => bus.AddTransport(new InMemoryConfig()));
            m.AddBus("audit", bus => bus.AddTransport(new InMemoryConfig()));
        });

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IBus>("audit").Name.ShouldBe("audit");

        var busProvider = provider.GetRequiredService<IBusProvider>();
        busProvider.GetBus("audit").Name.ShouldBe("audit");
        busProvider.Buses.Select(b => b.Name).ShouldBe([BusNames.Default, "audit"]);
    }

    [Fact]
    public void An_unknown_bus_name_throws_with_the_registered_names_listed()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m =>
            m.AddBus("events", bus => bus.AddTransport(new InMemoryConfig())));

        using var provider = services.BuildServiceProvider();
        var ex = Should.Throw<InvalidOperationException>(() =>
            provider.GetRequiredService<IBusProvider>().GetBus("nope"));
        ex.Message.ShouldContain("'nope'");
        ex.Message.ShouldContain("events");
    }

    [Fact]
    public void Two_buses_of_the_same_broker_type_get_independent_transports()
    {
        var services = NewServices();
        services.AddRunaxMessaging(m =>
        {
            m.AddBus("one", bus => bus.AddTransport(new InMemoryConfig()));
            m.AddBus("two", bus => bus.AddTransport(new InMemoryConfig()));
        });

        using var provider = services.BuildServiceProvider();

        var one = provider.GetRequiredKeyedService<IMessagingTransport>("one");
        var two = provider.GetRequiredKeyedService<IMessagingTransport>("two");

        one.SystemName.ShouldBe(two.SystemName);
        one.ShouldNotBeSameAs(two);
    }
}
