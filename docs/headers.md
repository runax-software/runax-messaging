# Headers & interop

A message's metadata travels in **two places at once**: inside the JSON envelope under the
reserved `__runax` key (see [Serialization](serialization.md)), and as **native broker headers**
(AMQP headers, Kafka record headers, SQS message attributes, and so on). The envelope is
the complete copy that works everywhere. Native headers are what other languages, broker
tooling, and tracing instrumentation expect to find.

This makes a mixed fleet work in both directions. A Go, Java, or Node service can send plain
JSON with ordinary broker headers, and a Runax consumer routes it and continues its trace.
Runax messages, in turn, look normal to a consumer that reads only the body and headers.

## What is written

Every publish writes the same metadata to both places:

| Metadata | In the envelope | As a native header |
| --- | --- | --- |
| Your headers (`PublishAsync(..., headers)`) | `__runax.headers.<name>` | `<name>` |
| W3C trace context, when a trace is active | `__runax.headers.traceparent` / `tracestate` | `traceparent` / `tracestate` |
| `[MessageContract]` schema `$id` | `__runax.contract_schema` | `runax-contract-schema` |
| `[MessageContract]` name | `__runax.contract_name` | `runax-contract-name` |
| `[MessageContract]` version | `__runax.contract_version` | `runax-contract-version` |
| Dead-letter details | `__runax.headers.x-runax-dlq-*` | `x-runax-dlq-*` |

The native names are also available as constants on `MessageHeaders`. A message with no
headers, no contract, and no active trace carries no metadata at all. It is published as
plain JSON with no `__runax` key and no native headers.

## Where each broker puts them

| Transport | Native headers live in | Limits |
| --- | --- | --- |
| RabbitMQ | The AMQP headers table | — |
| Apache Kafka | Record headers, as UTF-8 | — |
| Azure Service Bus | `ApplicationProperties` | — |
| Azure Event Hubs | `EventData.Properties` | — |
| Google Pub/Sub | Message attributes | Keys ≤ 256 bytes and not starting with `goog`; values ≤ 1 KB; 100 per message |
| Amazon SQS | Message attributes (`String`) | **10 per message**; names `A-Z a-z 0-9 _ - .`; no empty values |
| Amazon SNS | Message attributes (`String`) | Same as SQS. On the subscribed queue they arrive as SQS attributes with raw message delivery, or inside the notification JSON without it; both are read |
| Redis Streams | Extra fields on the stream entry, next to `data` | A header named `data` stays envelope-only |
| In-memory | Carried with the message | — |

Where a broker caps or restricts headers, they are written most important first: contract
name and version, then `traceparent` and `tracestate`, then the rest by name. Whatever doesn't
fit stays in the envelope only, so nothing is lost.

[Custom transports](writing-a-custom-transport.md) that don't implement native headers keep
working: their messages carry metadata in the envelope only.

## Reading: merged, with configurable precedence

On receive, the broker's headers and the envelope's are merged **key by key** into
`MessageContext.Headers`, and the contract name and version are merged the same way. A key
present on only one side is taken from that side. A key present on both sides is resolved by
the bus's `HeaderPrecedence`:

```csharp
messaging.AddBus(bus =>
{
    bus.AddTransport(new KafkaConfig { BootstrapServers = "localhost:9092" });
    bus.HeaderPrecedence = HeaderSource.Envelope; // default: HeaderSource.Broker
});
```

- **`HeaderSource.Broker`** (the default): the native header wins. That's what producers outside
  this library, broker plugins, and tracing instrumentation set.
- **`HeaderSource.Envelope`**: the envelope's copy wins.

`runax-contract-name` / `runax-contract-version` set the context's `ContractName` /
`ContractVersion` and aren't repeated in `MessageContext.Headers`. A version header that
isn't an integer is ignored.

The broker headers merged here are the user-level ones listed in the table above, so
broker-managed properties such as delivery counts or timestamps stay out. SDK
instrumentation may add its own entries (for example Azure's `Diagnostic-Id`), and those
show up like any other header.

## Turning native headers off

To keep metadata in the envelope only, as before this feature, turn native headers off per
bus. Receiving still merges whatever native headers arrive.

```csharp
messaging.AddBus("legacy", bus =>
{
    bus.AddTransport(new SqsConfig { Region = "us-east-1" });
    bus.WriteNativeHeaders = false;
});
```

The setting covers every publish path on the bus: direct publishes, outbox dispatch, and
framework-managed dead-lettering.

## Producing from other languages

A non-.NET service doesn't need to know about `__runax`. It sends its payload as plain JSON
and sets ordinary broker headers:

- **Versioning:** set `runax-contract-version` (and optionally `runax-contract-name`). A
  Runax consumer declared with `[MessageContract(2)]` then receives the message exactly as
  if it came from a Runax producer.
- **Tracing:** OpenTelemetry instrumentation in Go, Java, and Node already puts
  `traceparent` / `tracestate` into broker headers on most brokers. The Runax consumer
  span joins that trace with no extra work.
- **Custom headers:** any other header shows up in `MessageContext.Headers`.

For example, with the Go Kafka client:

```go
producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Value:          []byte(`{"Id":7,"CustomerId":42}`),
    Headers: []kafka.Header{
        {Key: "runax-contract-version", Value: []byte("2")},
        {Key: "tenant", Value: []byte("acme")},
    },
}, nil)
```

Consuming the other way works the same: read the body as plain JSON (ignore `__runax`, or
read it when you need the metadata) and read the headers natively.

## Outbox and dead-letters

The outbox stores only the envelope. That's enough, because the envelope carries every
header, and the dispatcher rebuilds the native headers from it at delivery time, so the store
schema doesn't change. A framework-managed dead-letter copy carries the message's merged
headers natively plus the `x-runax-dlq-*` details, and its envelope gets the details as
well. Broker-native dead-lettering (RabbitMQ DLX, Service Bus DLQ, Kafka and Event Hubs
dead-letter topics) keeps the original native headers.

## See also

- [Serialization](serialization.md): the `__runax` envelope and body serializers
- [Publishing](publishing.md): headers on `PublishAsync`
- [Consuming & reliability](consuming.md): contract versioning and dead-lettering
- [Observability](observability.md): trace-context propagation
