using System.Diagnostics;

using Runax.Messaging.Cli.Contracts;

namespace Runax.Messaging.Cli.Tests;

/// <summary>A throwaway directory of schema files, optionally a git repository.</summary>
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
    public SchemaDirectory WriteSchema(string relativePath, string body, int? legacyVersion = null)
    {
        var id = $"https://contracts.test/{relativePath[..^".schema.json".Length]}";
        var legacy = legacyVersion is { } v ? $"\"x-runax-legacy-version\":{v}," : "";
        return Write(relativePath, $$"""{"$schema":"{{Draft}}","$id":"{{id}}",{{legacy}}{{body}}}""");
    }

    public SchemaDirectory InitGit()
    {
        Git("init", "--quiet");
        return this;
    }

    public SchemaDirectory Commit(string message)
    {
        Git("add", "--all");
        Git("-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", message);
        return this;
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

    private void Git(params string[] arguments)
    {
        var start = new ProcessStartInfo("git") { WorkingDirectory = Root, UseShellExecute = false, RedirectStandardError = true };
        foreach (var argument in arguments)
            start.ArgumentList.Add(argument);

        using var process = Process.Start(start)!;
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"git {string.Join(' ', arguments)} failed: {error}");
    }
}
