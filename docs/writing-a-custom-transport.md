# Writing a custom transport

A transport plugs a broker into Runax.Messaging. It does two things: implement
`IMessagingTransport`, and provide a `MessagingConfigurator` extension so callers
can register it. This guide builds a fictional `Runax.Messaging.Foo`.

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

## 2. Options

Expose broker configuration as a plain options class:

```csharp
namespace Runax.Messaging.Foo;

public sealed class FooOptions
{
    public string Endpoint { get; set; } = "localhost:1234";
}
```

## 3. Implement `IMessagingTransport`

The transport works entirely in terms of the serialized envelope string — it
never touches your message types. `onMessage` returns a `MessageDisposition`
telling you what to do with the message once the dispatch pipeline (deserialize,
retry, dead-letter) has finished with it.

```csharp
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Foo;

internal sealed class FooTransport(FooOptions options, ILogger<FooTransport> logger)
    : IMessagingTransport
{
    // Identifies the transport: the messaging.system telemetry tag, and the key used by
    // AddConsumer<T>("foo") / PublishTo("foo"). Expose it as a constant so registration can reference it.
    internal const string TransportName = "foo";

    public string SystemName => TransportName;

    public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
    {
        // send envelopeJson to `topic` on the broker
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

- `SystemName` is a short broker identifier (`"rabbitmq"`, `"sqs"`, `"foo"`) used
  as the `messaging.system` telemetry tag. It also identifies the transport when several are
  registered (for `AddConsumer<T>("foo")` and `PublishTo("foo")`), so make it unique and stable.
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

## 4. Add a configurator extension

This is how callers select your transport. Register the options and your
transport as `IMessagingTransport`:

Follow the built-in transports: take a single builder block so callers configure options (via
`Configure`) and register consumers in one place.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Foo;

public static class FooConfiguratorExtensions
{
    public static MessagingConfigurator AddFoo(
        this MessagingConfigurator configurator,
        Action<TransportBuilder<FooOptions>> configure)
    {
        var builder = new TransportBuilder<FooOptions>(configurator.Services, FooTransport.TransportName);
        configure(builder);

        var options = configurator.Services
            .AddOptions<FooOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();
        if (builder.Configuration is not null)
            options.Configure(builder.Configuration);

        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<FooOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, FooTransport>();

        return configurator;
    }
}
```

The core package registers `IMessagePublisher`, the serializer, and the consumer
host — your transport only contributes `IMessagingTransport` and its options.

## 5. Use it

```csharp
using Runax.Messaging;
using Runax.Messaging.Foo;

builder.Services.AddRunaxMessaging(messaging => messaging
    .AddFoo(foo => foo.Configure(o => o.Endpoint = "broker:1234"))
    .AddConsumer<OrderPlacedConsumer>());
```

## 6. Test it

Add a project under `tests/` and cover publish → subscribe round-tripping. The
in-memory transport in the core package (`InMemoryTransport`) is a compact
reference implementation to model yours on.
