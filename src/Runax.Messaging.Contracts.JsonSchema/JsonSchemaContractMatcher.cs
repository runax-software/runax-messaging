using System.Text.Json;
using Runax.Messaging.Consumers;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Core's <see cref="IContractSchemaMatcher"/>, backed by the loaded contract schemas: it supplies each line's
/// legacy version and validates unidentified message bodies against candidate lines.
/// </summary>
internal sealed class JsonSchemaContractMatcher(IContractSchemaProvider provider) : IContractSchemaMatcher
{
    public int? LegacyVersionOf(string contractSchema) =>
        provider.TryGet(contractSchema, out var document) ? document.LegacyVersion : null;

    public ContractMatch Match(string body, IReadOnlyCollection<string> candidates)
    {
        JsonDocument parsed;
        try
        {
            parsed = JsonDocument.Parse(body);
        }
        catch (JsonException)
        {
            return new ContractMatch([], ["the body is not JSON"]);
        }

        using (parsed)
        {
            var accepted = new List<string>();
            var failures = new List<string>();

            foreach (var candidate in candidates)
            {
                // Unknown and non-selective lines can't route; the startup check warns about both.
                if (!provider.TryGet(candidate, out var document) || !document.IsSelective)
                    continue;

                var result = document.Validate(parsed.RootElement);
                if (result.IsValid)
                    accepted.Add(candidate);
                else
                    failures.Add($"{candidate} at {result.Errors[0]}");
            }

            return new ContractMatch(accepted, failures);
        }
    }
}
