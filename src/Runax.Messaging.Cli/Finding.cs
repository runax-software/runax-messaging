namespace Runax.Messaging.Cli;

/// <summary>How serious a <see cref="Finding"/> is. Errors fail the run.</summary>
internal enum Severity
{
    Warning,
    Error,
}

/// <summary>One problem the tool found in a schema file.</summary>
/// <param name="File">The file, relative to the directory the command was given.</param>
/// <param name="Pointer">Where in the schema, as a JSON pointer; empty for the whole file.</param>
/// <param name="Severity">Whether it fails the run.</param>
/// <param name="Message">What's wrong, and when it helps, what to do about it.</param>
internal sealed record Finding(string File, string Pointer, Severity Severity, string Message)
{
    public string ToText() =>
        $"{(Severity == Severity.Error ? "error" : "warning")}: {File}{(Pointer.Length > 0 ? $" at {Pointer}" : "")}: {Message}";

    // GitHub Actions workflow command, which annotates the file in a pull request.
    public string ToGitHub() =>
        $"::{(Severity == Severity.Error ? "error" : "warning")} file={File}::{(Pointer.Length > 0 ? $"{Pointer}: " : "")}{Message}";
}
