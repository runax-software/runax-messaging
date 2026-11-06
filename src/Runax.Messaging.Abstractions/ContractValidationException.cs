namespace Runax.Messaging.Abstractions;

/// <summary>
/// Thrown when a message body fails validation against its contract's JSON Schema. On consume it is
/// poison: the message is dead-lettered without retries, because validation is deterministic. Thrown the
/// same way whichever layer validated, the library or a transport's native validation.
/// </summary>
public sealed class ContractValidationException : PoisonMessageException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ContractValidationException"/> class.
    /// </summary>
    /// <param name="contractSchema">The <c>$id</c> of the schema the body was validated against.</param>
    /// <param name="errors">The validation failures, each a JSON pointer and a reason.</param>
    public ContractValidationException(string contractSchema, IReadOnlyList<string> errors)
        : base(FormatMessage(contractSchema, errors))
    {
        ContractSchema = contractSchema;
        Errors = errors;
    }

    /// <summary>Gets the <c>$id</c> of the schema the body was validated against.</summary>
    public string ContractSchema { get; }

    /// <summary>Gets the validation failures, each a JSON pointer and a reason.</summary>
    public IReadOnlyList<string> Errors { get; }

    private static string FormatMessage(string contractSchema, IReadOnlyList<string> errors) =>
        $"The message does not match contract schema '{contractSchema}': " +
        (errors.Count == 0 ? "invalid." : string.Join("; ", errors.Take(5)) + (errors.Count > 5 ? "; …" : "."));
}
