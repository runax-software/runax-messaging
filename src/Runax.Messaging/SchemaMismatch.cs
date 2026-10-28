namespace Runax.Messaging;

/// <summary>
/// What a bus does with a message whose contract schema <c>$id</c> doesn't match a consumer's, while the
/// message's contract version still does.
/// </summary>
public enum SchemaMismatch
{
    /// <summary>
    /// The message isn't delivered to that consumer: a mismatched <c>$id</c> is the drift schema contracts
    /// exist to catch.
    /// </summary>
    Reject,

    /// <summary>
    /// The message is delivered by its version, with a warning and the
    /// <c>runax.messaging.contract.schema_mismatch</c> metric. Meant as a safety valve while a contract is
    /// being adopted.
    /// </summary>
    Report,
}
