# Observability

Runax.Messaging is OpenTelemetry-ready with **zero SDK dependency**. Instrumentation
uses the in-box `System.Diagnostics` primitives — an `ActivitySource` for tracing and
a `Meter` for metrics — that stay silent until something subscribes to them. You opt
in by registering the well-known names with your telemetry pipeline (OpenTelemetry,
`dotnet-counters`, a custom `ActivityListener`/`MeterListener`); the library itself
never references an exporter.

Both names are constants on `MessagingDiagnostics` (in the core `Runax.Messaging`
package) and both are `"Runax.Messaging"`:

| Constant | Value | Subscribe with |
| --- | --- | --- |
| `MessagingDiagnostics.ActivitySourceName` | `Runax.Messaging` | `TracerProviderBuilder.AddSource("Runax.Messaging")` |
| `MessagingDiagnostics.MeterName` | `Runax.Messaging` | `MeterProviderBuilder.AddMeter("Runax.Messaging")` |

On top of traces and metrics, each bus auto-registers a broker-reachability
[health check](#health-checks) named `runax:{bus}`, and the dispatch pipeline writes
structured [log events](#logging) at every significant decision point.

## Tracing

### The producer span

Every `PublishAsync` / `PublishBatchAsync` call starts a span named
**`{topic} publish`** with `ActivityKind.Producer`:

| Tag | Value | Example |
| --- | --- | --- |
| `messaging.system` | The transport's `SystemName` | `rabbitmq`, `sqs`, `in-memory` |
| `messaging.destination.name` | The topic | `orders.placed` |
| `messaging.operation` | Always `publish` | `publish` |
| `messaging.runax.bus` | The bus name | `default`, `payments` |
| `messaging.batch.message_count` | Batch size — **`PublishBatchAsync` only** | `50` |

A batch publish produces **one** producer span for the whole batch (tagged with
`messaging.batch.message_count`), not one per message.

If the sink publish throws — broker unreachable, publisher confirm timeout — the span
status is set to `ActivityStatusCode.Error` with the exception message, and the
exception is rethrown to the caller.

### The consumer span

For each received message that reaches dispatch, the consumer hosted service starts a
span named **`{topic} process`** with `ActivityKind.Consumer` and the same tag set,
with `messaging.operation` = `process` (and no batch tag — consumption is per
message):

| Tag | Value |
| --- | --- |
| `messaging.system` | The transport's `SystemName` |
| `messaging.destination.name` | The topic |
| `messaging.operation` | `process` |
| `messaging.runax.bus` | The bus name |

The span covers consumer selection, every `HandleAsync` attempt including retries,
and dead-lettering. Its status is set to `ActivityStatusCode.Error` when the message
ends up dead-lettered (`"Message dead-lettered."`) or when the envelope cannot be
deserialized at all (a span is still started so the failure is visible in traces).

### Trace-context propagation

Runax.Messaging propagates W3C trace context **through the envelope headers**, so a
publish in one service and its processing in another land in the same distributed
trace:

- **Publish** — the producer span's context is injected into the outgoing headers via
  `DistributedContextPropagator.Current` (the standard `traceparent` / `tracestate`
  fields), which then travel inside the envelope's `__runax.headers`.
- **Process** — the consumer side extracts `traceparent` / `tracestate` from the
  received headers and starts the `{topic} process` span as a **child of the
  producer span**, carrying the trace state across.

The result: `HTTP request → orders publish → orders process → downstream work` shows
up as one trace across services, on every transport, with no broker-specific
instrumentation. Because the propagator is `DistributedContextPropagator.Current`,
the format follows whatever your host configures (W3C by default).

### Wiring up OpenTelemetry

The library only emits; your application subscribes. A typical ASP.NET Core setup
with the OTLP exporter:

```csharp
using Runax.Messaging.Diagnostics;

builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("orders-api"))
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddSource(MessagingDiagnostics.ActivitySourceName) // "Runax.Messaging"
        .AddOtlpExporter())
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddMeter(MessagingDiagnostics.MeterName)           // "Runax.Messaging"
        .AddOtlpExporter());
```

Prefer the constants over string literals — they are the public contract. Without a
listener the spans are never created (the `StartActivity` calls return `null`), so
the instrumentation costs nothing when telemetry is off.

## Metrics

All instruments live on the `"Runax.Messaging"` meter:

| Instrument | Type | Unit | Description |
| --- | --- | --- | --- |
| `runax.messaging.published` | Counter&lt;long&gt; | `{message}` | Number of messages published. |
| `runax.messaging.consumed` | Counter&lt;long&gt; | `{message}` | Number of messages successfully handled by a consumer. |
| `runax.messaging.failed` | Counter&lt;long&gt; | `{message}` | Number of messages that failed processing and were dead-lettered or dropped. |
| `runax.messaging.processing.duration` | Histogram&lt;double&gt; | `ms` | Time spent processing a received message before it is acknowledged, requeued, or dead-lettered. |

Every measurement carries the same three tags:

| Tag | Value |
| --- | --- |
| `messaging.system` | The transport's `SystemName` (e.g. `rabbitmq`) |
| `messaging.destination.name` | The topic |
| `messaging.runax.bus` | The bus name |

Recording semantics, straight from the pipeline:

- **`published`** increments after the sink accepts the envelope — by 1 per
  `PublishAsync`, by the batch size per `PublishBatchAsync`.
- **`consumed`** increments once per consumer that completes `HandleAsync`
  successfully (a topic with several matching consumers records several increments
  for one message).
- **`failed`** increments once whenever a message enters dead-lettering — retries
  exhausted, poison, malformed envelope, or an unroutable message whose strategy is
  dead-letter — regardless of whether it is then republished to the dead-letter
  topic, handed to the broker (`BrokerNative`), or dropped because dead-lettering is
  disabled.
- **`processing.duration`** records once per received message, from envelope
  deserialization until the final disposition — including all retry attempts and
  backoff delays, and including the malformed-envelope and unroutable paths.

Wire the meter into a `MeterProvider` (or use the `.WithMetrics` block above):

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("Runax.Messaging")
        .AddOtlpExporter());
```

Useful queries once the metrics land in Prometheus (names shown as the OTLP
exporter's default translation):

```promql
# Dead-letter/drop rate per bus — the first thing to alert on
sum by (messaging_runax_bus) (rate(runax_messaging_failed_total[5m]))

# p95 processing duration per topic on one bus
histogram_quantile(0.95, sum by (messaging_destination_name, le) (
  rate(runax_messaging_processing_duration_milliseconds_bucket{messaging_runax_bus="payments"}[5m])))

# Publish vs consume throughput per bus (a widening gap means a backlog is building)
sum by (messaging_runax_bus) (rate(runax_messaging_published_total[5m]))
sum by (messaging_runax_bus) (rate(runax_messaging_consumed_total[5m]))
```

## The bus tag

`messaging.runax.bus` appears on every span and every measurement, and it exists
because `messaging.system` alone cannot tell two buses apart. In 2.0 a bus is a named
messaging context wrapping exactly one transport — and nothing stops two buses from
using the **same broker technology**: a `payments` bus and an `analytics` bus can
both be RabbitMQ, each with its own cluster, credentials, and policies. Both report
`messaging.system = "rabbitmq"`; only `messaging.runax.bus` says which is which.

Dashboard guidance: **slice by bus first**. The bus is the operational unit — one
connection, one health check, one hosted dispatcher — so failures, latency spikes,
and backlogs are bus-shaped. Group panels and alerts by `messaging.runax.bus`, then
drill into `messaging.destination.name` within a bus. Treat `messaging.system` as
descriptive metadata, not an identity. Apps that never call `AddBus` with a name get
the default bus, whose name (`BusNames.Default`) is `"default"`.

## Health checks

Every broker-backed transport auto-registers an ASP.NET Core health check for its
bus, named **`runax:{bus}`** — e.g. `runax:default`, `runax:payments`. One bus = one
transport = one check, so a multi-bus app gets one independently-failing check per
broker connection. Opt out per bus on the transport config:

```csharp
bus.AddTransport(new RabbitMqConfig
{
    Uri = "amqps://user:pass@rabbit.internal/orders",
    RegisterHealthCheck = false,
});
```

What each transport's check verifies:

| Transport | `messaging.system` | The check reports healthy when… |
| --- | --- | --- |
| RabbitMQ | `rabbitmq` | The broker connection is open. |
| Kafka | `kafka` | The cluster is reachable and returns at least one broker. |
| AWS SQS | `sqs` | The SQS endpoint is reachable. |
| AWS SNS | `aws_sns` | The SNS endpoint is reachable. |
| Azure Service Bus | `servicebus` | The namespace is reachable. |
| Azure Event Hubs | `azure-event-hubs` | The namespace is reachable (fetches event hub properties). |
| Google Pub/Sub | `google_pubsub` | The Pub/Sub service is reachable. |
| Redis | `redis` | The Redis server is reachable. |
| In-memory | `in-memory` | — no health check; `RegisterHealthCheck` is ignored. |

Each check also reports unhealthy with a descriptive message when the bus's transport
is not the expected type or the probe throws (the exception is attached to the
result).

Expose the checks over HTTP with the standard middleware. The registrations carry no
tags, so filter by the `runax:` name prefix to publish a messaging-only endpoint:

```csharp
var app = builder.Build();

app.MapHealthChecks("/health");                 // everything
app.MapHealthChecks("/health/messaging", new HealthCheckOptions
{
    Predicate = r => r.Name.StartsWith("runax:", StringComparison.Ordinal),
});
```

Point the liveness/readiness probe of a consuming service at the messaging endpoint:
a bus that cannot reach its broker is not ready to do its job.

## Logging

The dispatch pipeline logs through `ILogger` at each significant stage. The key
events, all written by the consumer hosted service unless noted:

| Level | Event | Meaning |
| --- | --- | --- |
| Information | `Bus '{Bus}': subscribing to {TopicCount} topic(s) on '{System}': {Topics}` | Startup — which topics this bus's dispatcher subscribed to. |
| Information | `Bus '{Bus}': no topics to subscribe to. No consumers registered any topics.` | Startup — the bus has consumers configured but none declared a topic; the dispatcher exits. |
| Error | `Malformed envelope on topic '{Topic}' (bus '{Bus}'). Dead-lettering.` | The envelope could not be deserialized; the raw message is dead-lettered. |
| Warning | `No consumer accepts contract version {Version} on topic '{Topic}' (bus '{Bus}').` | Unroutable message — the configured `IUnroutableMessageHandler` decides its fate. |
| Warning | `Consumer {Consumer} failed on '{Topic}' (attempt {Attempt}/{MaxAttempts}). Retrying in {Delay}.` | A retryable failure; includes the attempt count and backoff delay. |
| Warning | `Consumer {Consumer} rejected message on '{Topic}' as poison. Dead-lettering.` | The consumer threw `PoisonMessageException` — retries are skipped. |
| Error | `Consumer {Consumer} failed on '{Topic}' after {Attempt} attempt(s). Dead-lettering.` | Retries exhausted; the message is handed to dead-lettering. |
| Warning | `Dead-lettering disabled; dropping message from '{Topic}'.` | `EnableDeadLettering = false` — the failed message is acknowledged and lost. |
| Information | `Rejecting message from '{Topic}' for broker-native dead-lettering after {Attempts} attempt(s).` | `DeadLetterStrategy.BrokerNative` — the broker's DLQ facility takes over. |
| Information | `Dead-lettered message from '{Topic}' to '{DeadLetterTopic}'.` | Framework-managed dead-letter publish succeeded. |
| Error | `Failed to dead-letter message from '{Topic}' to '{DeadLetterTopic}'. Requeueing.` | The dead-letter publish itself failed; the original message is requeued rather than lost. |
| Information | `Outbox dispatcher for bus '{Bus}' started, polling every {Interval}.` | Outbox — the per-bus dispatcher started. |
| Error | `Outbox dispatch for bus '{Bus}' failed; retrying after {Interval}.` | Outbox — a poll/publish cycle failed; pending rows stay pending and are retried next poll. |
| Information | `Outbox dispatcher for bus '{Bus}' shutting down.` | Outbox — clean shutdown. |

All messages are structured (named placeholders), so `Bus`, `Topic`, `Consumer`, and
`Attempt` are queryable fields in any structured logging backend.

Categories to raise or lower:

- `Runax.Messaging.Consumers.MessageConsumerHostedService` — the dispatch pipeline
  (everything from subscribe to dead-letter above).
- `Runax.Messaging.Outbox.OutboxDispatcher` — the outbox drain loop.
- `Runax.Messaging.Transports.*` — broker-level connection and channel logging from
  the individual transports.

A sensible production baseline: keep `Runax.Messaging` at `Information` (startup,
dead-letter outcomes), and rely on the `Warning`-level retry events as the early
signal for a struggling consumer — a rising rate of retry warnings usually precedes a
rising `runax.messaging.failed` count.

## See also

- [Buses](buses.md) — naming buses, the default bus, multi-bus topologies.
- [Consuming messages](consuming.md) — consumers, retries, dead-lettering, unroutable handling.
- [Publishing messages](publishing.md) — the publish pipeline, batching, headers.
- [Architecture & message flow](architecture.md) — where telemetry hooks into the pipeline.
