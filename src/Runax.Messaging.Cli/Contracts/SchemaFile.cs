using System.Text.Json;
using Runax.Messaging.Contracts.JsonSchema;

namespace Runax.Messaging.Cli.Contracts;

/// <summary>A <c>*.schema.json</c> file under the directory a command was given.</summary>
internal sealed class SchemaFile
{
    private SchemaFile(string path, string relativePath, string json)
    {
        FullPath = path;
        RelativePath = relativePath;
        Json = json;
    }

    public string FullPath { get; }

    /// <summary>Gets the path relative to the schema directory, with forward slashes.</summary>
    public string RelativePath { get; }

    public string Json { get; }

    /// <summary>Gets every schema file under <paramref name="directory"/>, in a stable order.</summary>
    public static IReadOnlyList<SchemaFile> FindAll(string directory)
    {
        var root = Path.GetFullPath(directory);
        return Directory.EnumerateFiles(root, "*.schema.json", SearchOption.AllDirectories)
            .Select(path => new SchemaFile(
                path,
                Path.GetRelativePath(root, path).Replace(Path.DirectorySeparatorChar, '/'),
                File.ReadAllText(path)))
            .OrderBy(file => file.RelativePath, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>Loads the document, reporting why it can't be loaded instead of throwing.</summary>
    public ContractSchemaDocument? TryLoad(List<Finding> findings) => TryLoad(Json, RelativePath, findings);

    public static ContractSchemaDocument? TryLoad(string json, string file, List<Finding> findings)
    {
        try
        {
            return ContractSchemaDocument.Parse(json, file);
        }
        catch (ContractSchemaException ex)
        {
            findings.Add(new Finding(file, "", Severity.Error, ex.Message));
            return null;
        }
    }

    /// <summary>Parses the raw JSON, reporting invalid JSON instead of throwing.</summary>
    public JsonDocument? TryParse(List<Finding> findings)
    {
        try
        {
            return JsonDocument.Parse(Json);
        }
        catch (JsonException ex)
        {
            findings.Add(new Finding(RelativePath, "", Severity.Error, $"not valid JSON: {ex.Message}"));
            return null;
        }
    }
}
