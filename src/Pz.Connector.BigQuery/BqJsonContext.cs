using System.Text.Json.Serialization;

namespace Pz.Connector.BigQuery;

// Every JSON call in this connector passes BqJsonContext.Default.<Type> explicitly -- no
// reflection-based serialization survives Native AOT trimming. CamelCase matches BigQuery's own
// wire naming for every DTO in BqDtos.cs without a per-property JsonPropertyName override;
// WhenWritingNull keeps a request body free of the fields this connector did not set (BigQuery
// mostly treats an omitted field and an explicit null the same, but a merge/patch endpoint like
// tables.patch would treat an explicit null as "clear this field" -- so absence, not null, is what
// this connector always sends).
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(BqTableReference))]
[JsonSerializable(typeof(BqFieldSchema))]
[JsonSerializable(typeof(BqTableSchema))]
[JsonSerializable(typeof(BqTable))]
[JsonSerializable(typeof(BqTablePatch))]
[JsonSerializable(typeof(BqJobReference))]
[JsonSerializable(typeof(BqErrorProto))]
[JsonSerializable(typeof(BqJobStatus))]
[JsonSerializable(typeof(BqLoadConfig))]
[JsonSerializable(typeof(BqQueryConfig))]
[JsonSerializable(typeof(BqJobConfiguration))]
[JsonSerializable(typeof(BqJob))]
[JsonSerializable(typeof(BqDatasetListEntry))]
[JsonSerializable(typeof(BqDatasetList))]
[JsonSerializable(typeof(BqErrorBody))]
[JsonSerializable(typeof(BqErrorEnvelope))]
internal sealed partial class BqJsonContext : JsonSerializerContext;
