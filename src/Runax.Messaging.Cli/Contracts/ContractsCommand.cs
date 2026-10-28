using System.CommandLine;

namespace Runax.Messaging.Cli.Contracts;

/// <summary>
/// <c>runax-messaging contracts</c>: the CI gate for JSON Schema contract files.
/// </summary>
internal static class ContractsCommand
{
    public static Command Create(TextWriter output, TextWriter error) =>
        new("contracts", "Lint and check JSON Schema contract files.")
        {
            Lint(output, error),
            Check(output, error),
        };

    private static Command Lint(TextWriter output, TextWriter error)
    {
        var directory = SchemaDirectory();
        var format = Format();
        var command = new Command(
            "lint",
            "Check each *.schema.json against the contract profile: a 2020-12 '$schema', an absolute '$id' that " +
            "matches the file path, supported keywords only, an open root object, and unique '$id's and legacy versions.")
        {
            directory,
            format,
        };

        command.SetAction(parseResult => Execute(
            "lint",
            parseResult.GetValue(format) == "github",
            output,
            error,
            () =>
            {
                var files = SchemaFile.FindAll(parseResult.GetRequiredValue(directory).FullName);
                return (files.Count, Linter.Lint(files));
            }));

        return command;
    }

    private static Command Check(TextWriter output, TextWriter error)
    {
        var directory = SchemaDirectory();
        var format = Format();
        var history = new Option<string>("--history")
        {
            Description = "Where earlier revisions come from. Only 'git' (the default) is supported; it needs the " +
                          "full history, e.g. fetch-depth: 0.",
        };
        history.AcceptOnlyFromAmong("git");
        var baseline = new Option<DirectoryInfo>("--baseline")
        {
            Description = "Compare against the schema files in this directory (for example a checkout of the base " +
                          "branch) instead of git history.",
        };
        baseline.AcceptExistingOnly();

        var command = new Command(
            "check",
            "Check that each schema file is fully compatible with every earlier revision of the same '$id', and " +
            "keeps its legacy version.")
        {
            directory,
            history,
            baseline,
            format,
        };
        command.Validators.Add(result =>
        {
            // Presence only: reading the values would throw when one of them is itself invalid.
            if (result.GetResult(history) is not null && result.GetResult(baseline) is not null)
                result.AddError("--history and --baseline can't be combined.");
        });

        command.SetAction(parseResult => Execute(
            "check",
            parseResult.GetValue(format) == "github",
            output,
            error,
            () =>
            {
                var root = parseResult.GetRequiredValue(directory).FullName;
                var files = SchemaFile.FindAll(root);
                var findings = parseResult.GetValue(baseline) is { } earlier
                    ? Checker.CheckBaseline(files, SchemaFile.FindAll(earlier.FullName))
                    : Checker.CheckHistory(files, GitHistory.Open(root));
                return (files.Count, findings);
            }));

        return command;
    }

    private static Argument<DirectoryInfo> SchemaDirectory()
    {
        var directory = new Argument<DirectoryInfo>("schema-dir")
        {
            Description = "The directory of *.schema.json files. File paths below it must match each '$id'.",
        };
        directory.AcceptExistingOnly();
        return directory;
    }

    private static Option<string> Format()
    {
        var format = new Option<string>("--format")
        {
            Description = "'text' (the default) or 'github' for GitHub Actions annotations.",
            DefaultValueFactory = _ => "text",
        };
        format.AcceptOnlyFromAmong("text", "github");
        return format;
    }

    private static int Execute(
        string command,
        bool gitHub,
        TextWriter output,
        TextWriter error,
        Func<(int Files, List<Finding> Findings)> run)
    {
        try
        {
            var (files, findings) = run();
            foreach (var finding in findings)
                output.WriteLine(gitHub ? finding.ToGitHub() : finding.ToText());

            var errors = findings.Count(f => f.Severity == Severity.Error);
            output.WriteLine($"contracts {command}: {files} schema file(s), {errors} error(s), {findings.Count - errors} warning(s).");
            return errors > 0 ? ExitCodes.Findings : ExitCodes.Success;
        }
        catch (Exception ex) when (ex is InvalidOperationException or IOException or UnauthorizedAccessException)
        {
            error.WriteLine($"runax-messaging: {ex.Message}");
            return ExitCodes.Usage;
        }
    }
}
