using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Runax.Messaging.Diagnostics;

/// <summary>
/// Telemetry primitives for Runax.Messaging. Consumers observe them by subscribing an
/// OpenTelemetry (or native) listener to the <see cref="ActivitySourceName"/> activity source
/// and the <see cref="MeterName"/> meter.
/// </summary>
public static class MessagingDiagnostics
{
    /// <summary>
    /// The name of the <see cref="System.Diagnostics.ActivitySource"/> that emits publish and
    /// consume spans. Register it with <c>TracerProviderBuilder.AddSource("Runax.Messaging")</c>.
    /// </summary>
    public const string ActivitySourceName = "Runax.Messaging";

    /// <summary>
    /// The name of the <see cref="System.Diagnostics.Metrics.Meter"/> that emits messaging metrics.
    /// Register it with <c>MeterProviderBuilder.AddMeter("Runax.Messaging")</c>.
    /// </summary>
    public const string MeterName = "Runax.Messaging";

    private static readonly string? Version = typeof(MessagingDiagnostics).Assembly.GetName().Version?.ToString();

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName, Version);

    internal static readonly Meter Meter = new(MeterName, Version);

    internal static readonly Counter<long> Published = Meter.CreateCounter<long>(
        "runax.messaging.published",
        unit: "{message}",
        description: "Number of messages published.");

    internal static readonly Counter<long> Consumed = Meter.CreateCounter<long>(
        "runax.messaging.consumed",
        unit: "{message}",
        description: "Number of messages successfully handled by a consumer.");

    internal static readonly Counter<long> Failed = Meter.CreateCounter<long>(
        "runax.messaging.failed",
        unit: "{message}",
        description: "Number of messages that failed processing and were dead-lettered or dropped.");

    internal static readonly Histogram<double> ProcessingDuration = Meter.CreateHistogram<double>(
        "runax.messaging.processing.duration",
        unit: "ms",
        description: "Time spent processing a received message before it is acknowledged, requeued, or dead-lettered.");

    internal static readonly Counter<long> ContractAmbiguous = Meter.CreateCounter<long>(
        "runax.messaging.contract.ambiguous",
        unit: "{message}",
        description: "Number of unidentified messages that matched contract schemas reaching different consumers.");

    internal static readonly Counter<long> ContractSchemaMismatch = Meter.CreateCounter<long>(
        "runax.messaging.contract.schema_mismatch",
        unit: "{message}",
        description: "Number of messages delivered by their contract version despite a mismatched schema $id.");

    internal static readonly Counter<long> ContractValidationFailures = Meter.CreateCounter<long>(
        "runax.messaging.contract.validation_failures",
        unit: "{message}",
        description: "Number of message bodies that failed validation against their contract schema.");

    internal static TagList Tags(string system, string destination, string bus) => new()
    {
        { "messaging.system", system },
        { "messaging.destination.name", destination },
        { "messaging.runax.bus", bus },
    };
}
