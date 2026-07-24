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
    /// Registers Redis Streams (Redis or Valkey) as the messaging transport, configuring options and consumers
    /// in one block: <c>AddRedis(redis =&gt; { redis.Configure(o =&gt; ...); redis.AddConsumer&lt;T&gt;(); })</c>.
    /// </summary>
    /// <param name="configurator">The messaging configurator.</param>
    /// <param name="configure">Block that configures <see cref="RedisOptions"/> (via <c>Configure</c>) and registers consumers.</param>
    /// <returns>The same configurator, to allow chaining.</returns>
    public static MessagingConfigurator AddRedis(
        this MessagingConfigurator configurator,
        Action<TransportBuilder<RedisOptions>> configure)
    {
        var builder = new TransportBuilder<RedisOptions>(configurator.Services, RedisTransport.TransportName);
        configure(builder);

        var options = configurator.Services
            .AddOptions<RedisOptions>()
            .ValidateDataAnnotations()
            .ValidateOnStart();
        if (builder.Configuration is not null)
            options.Configure(builder.Configuration);

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
