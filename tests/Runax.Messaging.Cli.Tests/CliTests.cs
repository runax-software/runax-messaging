using Runax.Messaging.Cli.Contracts;

namespace Runax.Messaging.Cli.Tests;

public class CliTests
{
    private const string Valid = """
        "type":"object","properties":{"id":{"type":"integer"}},"required":["id"]
        """;

    [Fact]
    public void Lint_exits_zero_without_errors()
    {
        using var dir = new SchemaDirectory().WriteSchema("orders/order-placed/v2.schema.json", Valid);

        var (exitCode, output, _) = Run("contracts", "lint", dir.Root);

        exitCode.ShouldBe(0);
        output.ShouldContain("contracts lint: 1 schema file(s), 0 error(s), 0 warning(s).");
    }

    [Fact]
    public void Lint_exits_one_with_errors_and_can_print_github_annotations()
    {
        using var dir = new SchemaDirectory().WriteSchema("orders/order-placed/v2.schema.json", """
            "requried":["id"]
            """);

        var (exitCode, output, _) = Run("contracts", "lint", dir.Root, "--format", "github");

        exitCode.ShouldBe(1);
        output.ShouldContain("::error file=orders/order-placed/v2.schema.json::/requried: unknown keyword 'requried'");
    }

    [Theory]
    [InlineData("frobnicate")]
    [InlineData("contracts", "frobnicate")]
    [InlineData("contracts", "lint")]
    [InlineData("contracts", "lint", "{dir}", "--strict")]
    [InlineData("contracts", "lint", "{dir}", "--format", "xml")]
    [InlineData("contracts", "lint", "/does/not/exist")]
    [InlineData("contracts", "check", "{dir}", "--history", "svn")]
    [InlineData("contracts", "check", "{dir}", "--history", "git", "--baseline", "{dir}")]
    public void Usage_errors_exit_two(params string[] args)
    {
        using var dir = new SchemaDirectory();

        var (exitCode, _, error) = Run([.. args.Select(a => a.Replace("{dir}", dir.Root))]);

        exitCode.ShouldBe(2);
        error.ShouldContain("runax-messaging --help");
    }

    [Fact]
    public void Check_outside_a_git_repository_explains_the_problem()
    {
        using var dir = new SchemaDirectory().WriteSchema("orders/order-placed/v2.schema.json", Valid);

        var (exitCode, _, error) = Run("contracts", "check", dir.Root);

        exitCode.ShouldBe(2);
        error.ShouldContain("is not in a git repository");
    }

    [Fact]
    public void Help_prints_usage()
    {
        var (exitCode, output, _) = Run("contracts", "lint", "--help");

        exitCode.ShouldBe(0);
        output.ShouldContain("<schema-dir>");
        output.ShouldContain("--format");
    }

    private static (int ExitCode, string Output, string Error) Run(params string[] args)
    {
        using var output = new StringWriter();
        using var error = new StringWriter();
        var exitCode = Program.Run(args, output, error);
        return (exitCode, output.ToString(), error.ToString());
    }
}
