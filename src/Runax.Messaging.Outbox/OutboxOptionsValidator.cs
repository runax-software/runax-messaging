using Microsoft.Extensions.Options;

namespace Runax.Messaging.Outbox;

/// <summary>
/// Source-generated DataAnnotations validator for <see cref="OutboxOptions"/> — the trim- and
/// native-AOT-safe replacement for the reflection-based <c>ValidateDataAnnotations()</c>.
/// </summary>
[OptionsValidator]
internal sealed partial class OutboxOptionsValidator : IValidateOptions<OutboxOptions>;
