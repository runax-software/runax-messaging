namespace Runax.Messaging.Contracts.JsonSchema.Tests;

/// <summary>A throwaway directory of schema files.</summary>
internal sealed class SchemaDirectory : IDisposable
{
    public const string Draft = "https://json-schema.org/draft/2020-12/schema";

    public SchemaDirectory()
    {
        Root = Path.Combine(Path.GetTempPath(), "runax-contracts-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Root);
    }

    public string Root { get; }

    /// <summary>Writes <paramref name="json"/> to <c>&lt;root&gt;/&lt;relativePath&gt;</c>.</summary>
    public SchemaDirectory Write(string relativePath, string json)
    {
        var path = Path.Combine(Root, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, json);
        return this;
    }

    /// <summary>Writes a contract-profile document whose $id matches its path.</summary>
    public SchemaDirectory WriteSchema(string relativePath, string body)
    {
        var id = $"https://contracts.test/{relativePath[..^".schema.json".Length]}";
        return Write(relativePath, $$"""{"$schema":"{{Draft}}","$id":"{{id}}",{{body}}}""");
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(Root, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
