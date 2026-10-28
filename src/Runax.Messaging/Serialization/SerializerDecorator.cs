using Runax.Messaging.Abstractions;

namespace Runax.Messaging.Serialization;

/// <summary>
/// Wraps the body serializer a bus resolves for a topic (per topic, per bus, or the default), registered
/// with <c>bus.DecorateSerializer(...)</c>. Decorators apply in registration order, innermost first.
/// </summary>
internal sealed record SerializerDecorator(Func<IServiceProvider, ISerializer, ISerializer> Decorate);
