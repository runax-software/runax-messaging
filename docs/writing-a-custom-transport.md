# Writing a custom transport

A transport plugs a broker into Runax.Messaging. It does two things: implement
`IMessagingTransport`, and ship a `TransportConfig`-derived config type so callers
can attach it to a bus with `bus.AddTransport(...)`. This guide builds a fictional
`Runax.Messaging.Foo`.

## 1. Create the project

Add a library under `src/` named `Runax.Messaging.<Broker>`. It inherits the
shared build settings from `src/Directory.Build.props`, so the `.csproj` only
needs its references:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <Description>Foo transport for Runax.Messaging.</Description>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="../Runax.Messaging.Abstractions/Runax.Messaging.Abstractions.csproj" />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
    <!-- your broker SDK, with its version declared in Directory.Packages.props -->
  </ItemGroup>
</Project>
```

Reference **only** `Runax.Messaging.Abstractions` — not the core package or other
transports.

## 2. Implement `IMessagingTransport`

The transport works entirely in terms of the serialized envelope string — it
never touches your message types. `onMessage` returns a `MessageDisposition`
telling you what to do with the message once the dispatch pipeline (deserialize,
retry, dead-letter) has finished with it.

```csharp
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Foo;

internal sealed class FooTransport(FooConfig config, ILogger<FooTransport> logger)
    : IMessagingTransport
{
    // The broker technology identifier, used as the messaging.system telemetry tag.
    // Expose it as a constant so the config's SystemName can reference it.
    internal const string TransportName = "foo";

    public string SystemName => TransportName;

    public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
    {
        // send envelopeJson to `topic` on the broker (config.Endpoint, ...)
        return ValueTask.CompletedTask;
    }

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask<MessageDisposition>> onMessage,
        CancellationToken cancellationToken = default)
    {
        // for each received message:
        //   var disposition = await onMessage(envelopeJson, topic);
        //   act on `disposition` (see the table below), then move on.
        // Block until cancellationToken is signaled.
        await Task.Delay(Timeout.Infinite, cancellationToken);
    }
}
```

Contract notes:

- `SystemName` is a short broker identifier (`"rabbitmq"`, `"sqs"`, `"foo"`) used as the
  `messaging.system` telemetry tag. It is **descriptive only** — a transport's identity is
  the bus it is attached to, so `SystemName` does not need to be unique (any number of
  buses can run your transport side by side).
- The constructor takes your config object directly — no `IOptions<>` indirection.
- `PublishAsync` receives the already-serialized envelope. Send it as-is.
- `SubscribeAsync` must run until cancellation and invoke `onMessage` with
  `(envelopeJson, topic)` for each message, then act on the returned
  `MessageDisposition`:

  | Disposition | Meaning | Typical broker action |
  | --- | --- | --- |
  | `Acknowledge` | Handled (or framework dead-lettered). | Remove the message (ack / delete). |
  | `Requeue` | Try again later. | Return for redelivery (nack requeue / leave hidden). |
  | `DeadLetter` | Give up; do not redeliver. | Reject to the broker's native DLQ, else drop. |

- If dispatch throws unexpectedly, treat it as `Requeue` and log it.
- `PublishBatchAsync` is a **default interface method** that publishes one at a time, so you get
  it for free. Override it only if your broker has a native batch API (as SQS and RabbitMQ do).

## 3. Derive `TransportConfig`

This is how callers select your transport — instead of shipping an `Add<Broker>` extension
method, you ship a config type. Put your broker settings on it (DataAnnotations are validated
when the caller's `AddBus` block completes) and implement the factory:

```csharp
using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Foo;

public sealed class FooConfig : TransportConfig
{
    [Required]
    public string Endpoint { get; set; } = "localhost:1234";

    public override string SystemName => FooTransport.TransportName;

    protected override IMessagingTransport CreateTransport(TransportContext context) =>
        new FooTransport(this, context.Services.GetRequiredService<ILogger<FooTransport>>());
}
```

- **`CreateTransport`** is called once per bus, after the config has passed validation.
  `TransportContext` carries the application `Services` (for loggers, broker SDK clients),
  the `BusName`, and the bus's `Mode`.
- **`TransportContext.Mode`** lets you skip building the unused side: create no producer /
  publish pool when the bus is `BusMode.ConsumeOnly`, and open no subscription resources
  when it is `BusMode.PublishOnly`. This also makes least-privilege broker credentials
  (read-only ACLs) fail fast instead of late.
- **`protected override`, not `protected internal override`.** The base members are declared
  `protected internal` in `Runax.Messaging.Abstractions`; because your transport lives in a
  different assembly, C# requires the overrides to be declared plain `protected` (the
  `internal` half of the accessibility does not carry across assemblies).

### Optional: extra registrations and a health check

Override `ConfigureServices` for additional service registrations — most commonly a health
check. The convention is one check per bus named `runax:{bus}`, gated on the inherited
`RegisterHealthCheck` property (default `true`) so callers can opt out:

```csharp
protected override void ConfigureServices(IServiceCollection services, string busName)
{
    if (!RegisterHealthCheck)
        return;

    services.AddHealthChecks().Add(new HealthCheckRegistration(
        $"runax:{busName}",
        sp => new FooHealthCheck(sp.GetRequiredKeyedService<IMessagingTransport>(busName)),
        failureStatus: null,
        tags: null));
}
```

The transport is registered as a **keyed** `IMessagingTransport` singleton under the bus name,
which is why the health check resolves it with `GetRequiredKeyedService<IMessagingTransport>(busName)`.

The core package registers `IBus`, the serializer, and the per-bus consumer host — your
transport package only contributes the transport and its config type.

## 4. Use it

```csharp
using Runax.Messaging;
using Runax.Messaging.Foo;

builder.Services.AddRunaxMessaging(messaging =>
{
    messaging.AddBus(bus =>
    {
        bus.AddTransport(new FooConfig { Endpoint = "broker:1234" });
        bus.AddConsumer<OrderPlacedConsumer>();
    });
});
```

The delegate and `IConfiguration`-binding overloads come for free because they are generic on
`TransportConfig`: `bus.AddTransport<FooConfig>(c => c.Endpoint = "broker:1234")` and
`bus.AddTransport<FooConfig>(configuration.GetSection("Foo"))` both work without any code in
your package.

## 5. Test it

Add a project under `tests/` and cover publish → subscribe round-tripping. The
in-memory transport in the core package (`InMemoryTransport`, with `InMemoryConfig`
as its config type) is a compact reference implementation to model yours on.
