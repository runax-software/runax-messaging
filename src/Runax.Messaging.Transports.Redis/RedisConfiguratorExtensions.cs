using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Transports.Redis;

/// <summary>
/// Configurator extensions for the Redis Streams transport.
/// </summary>
public static class RedisConfiguratorExtensions
{
    /// <summary>
    /// Registers Redis Streams (Redis or Valkey) as the messaging transport.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Action to configure <see cref="RedisOptions"/>.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddRedis(
        this MessagingConfigurator configurator,
        Action<RedisOptions> configure)
    {
        configurator.Services
            .AddOptions<RedisOptions>()
            .Configure(configure)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddRedisCore(configurator);
    }

    /// <summary>
    /// Registers Redis Streams as the messaging transport, binding <see cref="RedisOptions"/> from configuration.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configuration">The configuration section to bind options from.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddRedis(
        this MessagingConfigurator configurator,
        IConfiguration configuration)
    {
        configurator.Services
            .AddOptions<RedisOptions>()
            .Bind(configuration)
            .ValidateDataAnnotations()
            .ValidateOnStart();

        return AddRedisCore(configurator);
    }

    private static MessagingConfigurator AddRedisCore(MessagingConfigurator configurator)
    {
        configurator.Services.TryAddSingleton(sp => sp.GetRequiredService<IOptions<RedisOptions>>().Value);
        configurator.Services.AddSingleton<IMessagingTransport, RedisTransport>();

        return configurator;
    }
}
