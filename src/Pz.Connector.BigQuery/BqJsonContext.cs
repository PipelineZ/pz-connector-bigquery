using System.Text.Json.Serialization;

namespace Pz.Connector.BigQuery;

// A JsonSerializerContext with no [JsonSerializable] types does not compile: the source generator
// emits no override of the abstract GetTypeInfo/GeneratedSerializerOptions members, so the class is
// left abstract. One placeholder entry keeps the context buildable until later tasks add the real
// wire types.
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(string))]
internal sealed partial class BqJsonContext : JsonSerializerContext;
