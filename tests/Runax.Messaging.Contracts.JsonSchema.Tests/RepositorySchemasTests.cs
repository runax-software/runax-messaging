namespace Runax.Messaging.Contracts.JsonSchema.Tests;

/// <summary>
/// Dogfoods the linter on the repository's own committed schema files, which is the gate a consumer is
/// meant to put in its own test suite. Warnings are allowed: <c>misc/anything/v1</c> deliberately accepts
/// any JSON object, because the tests need a line that isn't selective.
/// </summary>
public class RepositorySchemasTests
{
    public static TheoryData<string> SchemaDirectories =>
    [
        "tests/Runax.Messaging.Contracts.JsonSchema.Tests/schemas",
        "tests/Runax.Messaging.AotSmoke/schemas",
    ];

    [Theory]
    [MemberData(nameof(SchemaDirectories))]
    public void The_repositorys_schemas_pass_the_contract_profile(string directory)
    {
        var report = ContractSchemaLinter.Lint(Path.Combine(RepositoryRoot(), directory));

        report.Errors.ShouldBeEmpty(report.ToString());
    }

    /// <summary>Walks up from the test binary to the directory holding the solution.</summary>
    private static string RepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "Runax.Messaging.slnx")))
                return directory.FullName;
        }

        throw new InvalidOperationException(
            $"No Runax.Messaging.slnx above '{AppContext.BaseDirectory}'; the repository root could not be found.");
    }
}
