using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Diagnostics;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Validates bodies against their contract schemas around an inner <see cref="ISerializer"/>: after it
/// serializes on publish, and before it deserializes on consume. Types without a schema pass through.
/// </summary>
internal sealed class ValidatingSerializer(
    ISerializer inner,
    IContractSchemaProvider provider,
    ContractValidationOptions options,
    ILogger logger,
    string busName) : ISerializer
{
    private readonly ConcurrentDictionary<Type, bool> _warnedUnloaded = new();

    public string Serialize<TMessage>(TMessage message)
    {
        var json = inner.Serialize(message);

        if (options.OnPublish != ContractValidation.Off
            && MessageContractCache.For(typeof(TMessage))?.Schema is { } schema)
        {
            Check(typeof(TMessage), [ContractSchemaId.Normalize(schema)], json, options.OnPublish, "publish");
        }

        return json;
    }

    public TMessage? Deserialize<TMessage>(string body)
    {
        if (options.OnConsume != ContractValidation.Off)
        {
            var lines = MessageContractCache.SchemasFor(typeof(TMessage));
            if (lines.Count > 0)
                Check(typeof(TMessage), lines, body, options.OnConsume, "consume");
        }

        return inner.Deserialize<TMessage>(body);
    }

    private void Check(Type type, IReadOnlyCollection<string> lines, string json, ContractValidation mode, string direction)
    {
        var documents = lines
            .Select(line => provider.TryGet(line, out var document) ? document : null)
            .OfType<ContractSchemaDocument>()
            .ToArray();
        if (documents.Length == 0)
        {
            if (_warnedUnloaded.TryAdd(type, true))
            {
                logger.LogWarning(
                    "No contract schema is loaded for {Type} ({Schemas}); it isn't validated.",
                    type.Name, string.Join(", ", lines));
            }

            return;
        }

        using var parsed = JsonDocument.Parse(json);
        ContractValidationResult? firstFailure = null;
        foreach (var document in documents)
        {
            var result = document.Validate(parsed.RootElement);
            if (result.IsValid)
                return;

            firstFailure ??= result;
        }

        var failed = documents[0].Id;
        MessagingDiagnostics.ContractValidationFailures.Add(1, new TagList
        {
            { "messaging.runax.bus", busName },
            { "messaging.runax.contract.schema", failed },
            { "direction", direction },
        });

        if (mode == ContractValidation.Enforce)
            throw new ContractValidationException(failed, firstFailure!.Errors);

        logger.LogWarning(
            "{Type} failed validation against contract schema '{Schema}' on {Direction} (bus '{Bus}'): {Errors}",
            type.Name, failed, direction, busName, string.Join("; ", firstFailure!.Errors));
    }
}
