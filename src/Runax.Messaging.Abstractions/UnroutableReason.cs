namespace Runax.Messaging.Abstractions;

/// <summary>
/// Why no registered consumer accepted a message.
/// </summary>
public enum UnroutableReason
{
    /// <summary>
    /// The message carried a contract identity (a schema <c>$id</c> or a version) that no consumer on the
    /// topic accepts, or it carried none and only versioned consumers are registered.
    /// </summary>
    NoMatchingContract,

    /// <summary>
    /// The message carried no contract identity, and its body validated against none of the contract
    /// schemas the topic's consumers declare.
    /// </summary>
    NoMatchingSchema,

    /// <summary>
    /// The message carried no contract identity, and its body validated against several contract schemas
    /// that reach different consumers, so it was not guessed.
    /// </summary>
    AmbiguousSchema,
}
