namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Thrown when a contract schema document can't be loaded: it isn't valid JSON, lacks an absolute
/// <c>$id</c>, conflicts with another document's <c>$id</c>, or uses something outside the contract profile.
/// </summary>
public sealed class ContractSchemaException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="ContractSchemaException"/> class.</summary>
    /// <param name="message">What is wrong with the document.</param>
    public ContractSchemaException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ContractSchemaException"/> class.</summary>
    /// <param name="message">What is wrong with the document.</param>
    /// <param name="innerException">The underlying failure.</param>
    public ContractSchemaException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
