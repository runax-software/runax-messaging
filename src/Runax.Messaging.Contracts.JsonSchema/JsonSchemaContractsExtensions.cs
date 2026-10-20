using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Runax.Messaging.Abstractions;
using Runax.Messaging.Consumers;

namespace Runax.Messaging.Contracts.JsonSchema;

/// <summary>Registers JSON Schema contracts with Runax.Messaging.</summary>
public static class JsonSchemaContractsExtensions
{
    /// <summary>
    /// Loads contract schema documents and turns on what they enable: routing messages that carry no contract
    /// identity by the schemas their consumers declare (unless a bus sets <c>RequireContractIdentity</c>),
    /// accepting the legacy versions schema lines record, startup warnings, and <c>bus.ValidateContracts</c>.
    /// </summary>
    /// <param name="messaging">The messaging configurator.</param>
    /// <param name="configure">Adds the schema sources.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddJsonSchemaContracts(
        this MessagingConfigurator messaging,
        Action<JsonSchemaContractsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new JsonSchemaContractsBuilder();
        configure(builder);

        var services = messaging.Services;
        foreach (var source in builder.Sources)
            services.AddSingleton(source);

        services.TryAddSingleton<IContractSchemaProvider, ContractSchemaRegistry>();
        services.TryAddSingleton<IContractSchemaMatcher, JsonSchemaContractMatcher>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, ContractSchemaStartupCheck>());
        return messaging;
    }

    /// <summary>
    /// Validates message bodies on this bus against their contract schemas. Requires
    /// <see cref="AddJsonSchemaContracts"/>. Only types whose <c>[MessageContract]</c> declares a
    /// <c>Schema</c> are validated.
    /// </summary>
    /// <param name="bus">The bus builder.</param>
    /// <param name="configure">Sets validation per direction.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public static BusBuilder ValidateContracts(this BusBuilder bus, Action<ContractValidationOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new ContractValidationOptions();
        configure(options);

        var busName = bus.BusName;
        return bus.DecorateSerializer((services, inner) => new ValidatingSerializer(
            inner,
            services.GetService<IContractSchemaProvider>()
                ?? throw new InvalidOperationException(
                    $"Bus '{busName}' validates contracts, but no contract schemas are loaded. " +
                    "Call messaging.AddJsonSchemaContracts(...) to load them."),
            options,
            services.GetService<ILoggerFactory>()?.CreateLogger("Runax.Messaging.Contracts.JsonSchema")
                ?? NullLogger.Instance,
            busName));
    }
}

/// <summary>Collects the places contract schema documents are loaded from.</summary>
public sealed class JsonSchemaContractsBuilder
{
    internal List<ContractSchemaSource> Sources { get; } = [];

    /// <summary>
    /// Loads every manifest resource of <paramref name="assembly"/> whose name ends in <c>.schema.json</c>.
    /// Embed the committed files with <c>&lt;EmbeddedResource Include="schemas/**/*.schema.json" /&gt;</c>.
    /// </summary>
    /// <param name="assembly">The assembly that embeds the documents.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public JsonSchemaContractsBuilder AddEmbeddedSchemas(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        Sources.Add(new ContractSchemaSource.Embedded(assembly));
        return this;
    }

    /// <summary>Loads one document given as JSON text.</summary>
    /// <param name="json">The schema document.</param>
    /// <returns>The same builder, to allow chaining.</returns>
    public JsonSchemaContractsBuilder AddSchema(string json)
    {
        ArgumentNullException.ThrowIfNull(json);
        Sources.Add(new ContractSchemaSource.Inline(json));
        return this;
    }
}
