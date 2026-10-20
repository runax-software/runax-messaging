using System.CommandLine;
using Runax.Messaging.Cli.Contracts;

namespace Runax.Messaging.Cli;

/// <summary>
/// The <c>runax-messaging</c> command line. Commands are grouped by area (<c>contracts</c> today) so new areas
/// can be added as sibling groups.
/// </summary>
internal static class CliApp
{
    public static int Run(string[] args, TextWriter output, TextWriter error)
    {
        var root = new RootCommand("Tooling for Runax.Messaging.")
        {
            ContractsCommand.Create(output, error),
        };

        // Usage mistakes exit with 2, so CI can tell them apart from findings (1).
        var parseResult = root.Parse(args);
        if (parseResult.Errors.Count > 0)
        {
            foreach (var parseError in parseResult.Errors)
                error.WriteLine($"runax-messaging: {parseError.Message}");
            error.WriteLine("Run 'runax-messaging --help' for usage.");
            return ExitCodes.Usage;
        }

        return parseResult.Invoke(new InvocationConfiguration { Output = output, Error = error });
    }
}

/// <summary>The tool's exit codes.</summary>
internal static class ExitCodes
{
    /// <summary>No errors (warnings may have been printed).</summary>
    public const int Success = 0;

    /// <summary>The command found errors.</summary>
    public const int Findings = 1;

    /// <summary>The command line or environment was wrong, e.g. a missing directory or no git repository.</summary>
    public const int Usage = 2;
}
