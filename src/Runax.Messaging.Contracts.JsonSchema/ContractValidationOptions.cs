namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>What happens when a message body fails validation against its contract schema.</summary>
public enum ContractValidation
{
    /// <summary>No validation.</summary>
    Off,

    /// <summary>
    /// Log a warning and record <c>runax.messaging.contract.validation_failures</c>, then carry on.
    /// </summary>
    Report,

    /// <summary>
    /// Throw <see cref="Abstractions.ContractValidationException"/>: on publish, before anything reaches the
    /// transport or the outbox; on consume, so the message is dead-lettered as poison without retries.
    /// </summary>
    Enforce,
}

/// <summary>
/// Library validation for one bus, configured with <c>bus.ValidateContracts(...)</c>. Only message types
/// whose <c>[MessageContract]</c> declares a <c>Schema</c> are validated.
/// </summary>
public sealed class ContractValidationOptions
{
    /// <summary>Gets or sets validation of outgoing bodies against the type's <c>Schema</c>. Defaults to off.</summary>
    public ContractValidation OnPublish { get; set; } = ContractValidation.Off;

    /// <summary>
    /// Gets or sets validation of incoming bodies against the type's <c>Schema</c> and <c>Reads</c> lines; the
    /// body passes if any one of them accepts it. Defaults to off.
    /// </summary>
    public ContractValidation OnConsume { get; set; } = ContractValidation.Off;
}
