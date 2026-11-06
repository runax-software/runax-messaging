using System.Text.Json;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>A <c>*.schema.json</c> file under a schema directory.</summary>
internal sealed class SchemaFile
{
    private SchemaFile(string relativePath, string json)
    {
        RelativePath = relativePath;
        Json = json;
    }

    /// <summary>Gets the path relative to the schema directory, with forward slashes.</summary>
    public string RelativePath { get; }

    public string Json { get; }

    /// <summary>Gets every schema file under <paramref name="directory"/>, in a stable order.</summary>
    /// <exception cref="DirectoryNotFoundException"><paramref name="directory"/> does not exist.</exception>
    public static IReadOnlyList<SchemaFile> FindAll(string directory)
    {
        var root = Path.GetFullPath(directory);
        if (!Directory.Exists(root))
            throw new DirectoryNotFoundException($"The schema directory '{directory}' does not exist.");

        return
        [
            .. Directory.EnumerateFiles(root, "*.schema.json", SearchOption.AllDirectories)
                .Select(path => new SchemaFile(
                    Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/'),
                    File.ReadAllText(path)))
                .OrderBy(file => file.RelativePath, StringComparer.Ordinal),
        ];
    }

    /// <summary>Loads the document, reporting why it can't be loaded instead of throwing.</summary>
    public ContractSchemaDocument? TryLoad(List<ContractSchemaFinding> findings) =>
        TryLoad(Json, RelativePath, findings);

    public static ContractSchemaDocument? TryLoad(string json, string file, List<ContractSchemaFinding> findings)
    {
        try
        {
            return ContractSchemaDocument.Parse(json, file);
        }
        catch (ContractSchemaException ex)
        {
            findings.Add(new ContractSchemaFinding(file, "", ContractSchemaSeverity.Error, ex.Message));
            return null;
        }
    }

    /// <summary>Parses the raw JSON, reporting invalid JSON instead of throwing.</summary>
    public JsonDocument? TryParse(List<ContractSchemaFinding> findings)
    {
        try
        {
            return JsonDocument.Parse(Json);
        }
        catch (JsonException ex)
        {
            findings.Add(new ContractSchemaFinding(
                RelativePath, "", ContractSchemaSeverity.Error, $"not valid JSON: {ex.Message}"));
            return null;
        }
    }
}
