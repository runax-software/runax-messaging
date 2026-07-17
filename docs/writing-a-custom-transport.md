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
never touches your message types.

```csharp
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Foo;

internal sealed class FooTransport(FooOptions options, ILogger<FooTransport> logger)
    : IMessagingTransport
{
    public ValueTask PublishAsync(string topic, string envelopeJson, CancellationToken cancellationToken = default)
    {
        // send envelopeJson to `topic` on the broker
        return ValueTask.CompletedTask;
    }

    public async Task SubscribeAsync(
        string[] topics,
        Func<string, string, ValueTask> onMessage,
        CancellationToken cancellationToken = default)
    {
        // for each received message call: await onMessage(envelopeJson, topic);
        // then acknowledge it. Block until cancellationToken is signaled.
        await Task.Delay(Timeout.Infinite, cancellationToken);
    }
}
```

Contract notes:

- `PublishAsync` receives the already-serialized envelope. Send it as-is.
- `SubscribeAsync` must run until cancellation and invoke `onMessage` with
  `(envelopeJson, topic)` for each message.
- Acknowledge only after `onMessage` completes. On failure, decide your
  redelivery policy (requeue / visibility timeout / drop) and log it.

## 4. Add a configurator extension

This is how callers select your transport. Register the options and your
transport as `IMessagingTransport`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Foo;

public static class FooConfiguratorExtensions
{
    public static MessagingConfigurator AddFoo(
        this MessagingConfigurator configurator,
        Action<FooOptions> configure)
    {
        var options = new FooOptions();
        configure(options);

        configurator.Services.AddSingleton(options);
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
    .AddFoo(o => o.Endpoint = "broker:1234")
    .AddConsumer<OrderPlacedConsumer>());
```

## 6. Test it

Add a project under `tests/` and cover publish → subscribe round-tripping. The
in-memory transport in the core package (`InMemoryTransport`) is a compact
reference implementation to model yours on.
