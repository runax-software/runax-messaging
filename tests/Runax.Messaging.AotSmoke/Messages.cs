using System.Text.Json.Serialization;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.AotSmoke;

[MessageContract(2, Name = "orders.placed")]
public sealed record OrderPlaced(int Id, string Product);

public sealed record InvoiceIssued(int OrderId, decimal Amount);

/// <summary>
/// Source-generated JSON metadata for every message type. Native AOT disables reflection-based
/// System.Text.Json, so this is the only way bodies serialize.
/// </summary>
[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(InvoiceIssued))]
internal sealed partial class SmokeJsonContext : JsonSerializerContext;
