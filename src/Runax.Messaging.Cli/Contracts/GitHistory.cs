using System.Diagnostics;

namespace Runax.Messaging.Cli.Contracts;

/// <summary>
/// Reads earlier revisions of a file from git. CI checkouts need the full history (<c>fetch-depth: 0</c>);
/// a shallow clone only sees the revisions it fetched.
/// </summary>
internal sealed class GitHistory(string repositoryRoot)
{
    public string RepositoryRoot { get; } = repositoryRoot;

    /// <summary>Finds the repository containing <paramref name="directory"/>.</summary>
    /// <exception cref="InvalidOperationException">The directory isn't in a git repository, or git isn't installed.</exception>
    public static GitHistory Open(string directory)
    {
        var (exitCode, output, error) = Git(directory, "rev-parse", "--show-toplevel");
        return exitCode == 0
            ? new GitHistory(output.Trim())
            : throw new InvalidOperationException($"'{directory}' is not in a git repository: {error.Trim()}");
    }

    /// <summary>
    /// Returns the committed contents of <paramref name="fullPath"/> at every commit that changed it, newest
    /// first, following renames.
    /// </summary>
    public IEnumerable<(string Commit, string Json)> Revisions(string fullPath)
    {
        // Run from the file's directory with its bare name, so symlinked paths (macOS /var -> /private/var)
        // don't need reconciling with the repository root. --name-only then prints each commit's path for the
        // file relative to the root, which `git show <commit>:<path>` expects, and --follow tracks renames.
        var (exitCode, output, _) = Git(
            Path.GetDirectoryName(fullPath)!, "log", "--follow", "--format=%H", "--name-only", "--", Path.GetFileName(fullPath));
        if (exitCode != 0)
            yield break;

        var lines = output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        for (var i = 0; i + 1 < lines.Length; i += 2)
        {
            var (commit, pathAtCommit) = (lines[i], lines[i + 1]);
            var (showExit, json, _) = Git(RepositoryRoot, "show", $"{commit}:{pathAtCommit}");
            if (showExit == 0)
                yield return (commit, json);
        }
    }

    private static (int ExitCode, string Output, string Error) Git(string workingDirectory, params string[] arguments)
    {
        var start = new ProcessStartInfo("git")
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
            start.ArgumentList.Add(argument);

        try
        {
            using var process = Process.Start(start)!;
            var output = process.StandardOutput.ReadToEndAsync();
            var error = process.StandardError.ReadToEnd();
            process.WaitForExit();
            return (process.ExitCode, output.Result, error);
        }
        catch (System.ComponentModel.Win32Exception ex)
        {
            throw new InvalidOperationException($"git could not be started: {ex.Message}", ex);
        }
    }
}
