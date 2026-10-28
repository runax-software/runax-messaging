using System.Diagnostics.CodeAnalysis;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>
/// Looks up loaded contract schema documents by <c>$id</c>. Registered by <c>AddJsonSchemaContracts</c>.
/// </summary>
public interface IContractSchemaProvider
{
    /// <summary>Gets every loaded document.</summary>
    IReadOnlyCollection<ContractSchemaDocument> Documents { get; }

    /// <summary>Looks up the document whose <c>$id</c> is <paramref name="id"/>.</summary>
    /// <param name="id">The <c>$id</c>; an empty trailing fragment (<c>#</c>) is ignored.</param>
    /// <param name="document">The document, when found.</param>
    /// <returns>Whether the document is loaded.</returns>
    bool TryGet(string id, [NotNullWhen(true)] out ContractSchemaDocument? document);
}

/// <summary>One place contract schemas are loaded from.</summary>
internal abstract record ContractSchemaSource
{
    public abstract IEnumerable<(string Json, string Origin)> Read();

    /// <summary>Every <c>*.schema.json</c> manifest resource of an assembly.</summary>
    public sealed record Embedded(System.Reflection.Assembly Assembly) : ContractSchemaSource
    {
        public override IEnumerable<(string Json, string Origin)> Read()
        {
            foreach (var name in Assembly.GetManifestResourceNames())
            {
                if (!name.EndsWith(".schema.json", StringComparison.OrdinalIgnoreCase))
                    continue;

                using var stream = Assembly.GetManifestResourceStream(name)!;
                using var reader = new StreamReader(stream);
                yield return (reader.ReadToEnd(), $"'{name}' in {Assembly.GetName().Name}");
            }
        }
    }

    /// <summary>A document given as JSON text.</summary>
    public sealed record Inline(string Json) : ContractSchemaSource
    {
        public override IEnumerable<(string Json, string Origin)> Read() => [(Json, "added with AddSchema")];
    }
}

/// <summary>
/// The default <see cref="IContractSchemaProvider"/>: loads every registered source once, at first use.
/// </summary>
internal sealed class ContractSchemaRegistry : IContractSchemaProvider
{
    private readonly Dictionary<string, ContractSchemaDocument> _documents = new(StringComparer.Ordinal);

    public ContractSchemaRegistry(IEnumerable<ContractSchemaSource> sources)
    {
        foreach (var source in sources.Distinct())
        {
            foreach (var (json, origin) in source.Read())
            {
                var document = ContractSchemaDocument.Parse(json, origin);
                if (_documents.TryGetValue(document.Id, out var existing))
                {
                    if (existing.Json == document.Json)
                        continue;

                    throw new ContractSchemaException(
                        $"Two different contract schema documents declare '$id' '{document.Id}' (the second one is {origin}).");
                }

                _documents[document.Id] = document;
            }
        }
    }

    public IReadOnlyCollection<ContractSchemaDocument> Documents => _documents.Values;

    public bool TryGet(string id, [NotNullWhen(true)] out ContractSchemaDocument? document) =>
        _documents.TryGetValue(ContractSchemaId.Normalize(id), out document);
}
