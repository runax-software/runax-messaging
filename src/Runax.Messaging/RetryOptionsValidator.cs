using Microsoft.Extensions.Options;

namespace Runax.Messaging;

/// <summary>
/// Source-generated DataAnnotations validator for <see cref="RetryOptions"/> — the trim- and
/// native-AOT-safe replacement for the reflection-based <c>ValidateDataAnnotations()</c>.
/// </summary>
[OptionsValidator]
internal sealed partial class RetryOptionsValidator : IValidateOptions<RetryOptions>;
