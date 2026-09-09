namespace Pz.Connector.BigQuery;

// The BigQuery REST v2 wire shapes this connector actually touches (tables, jobs, error envelopes).
// Every member is nullable: a partial response (BigQuery omits absent fields; DefaultIgnoreCondition
// on BqJsonContext omits them on the way out too) must round-trip without a deserialization failure,
// and a positional record's constructor-parameter binding leaves any field missing from the JSON at
// its type's default -- null for every reference type here. Property names are PascalCase; the
// shared camelCase naming policy on BqJsonContext maps each one onto its wire name (ProjectId ->
// "projectId", ...) without needing a JsonPropertyName override anywhere in this file.

internal sealed record BqTableReference(string? ProjectId, string? DatasetId, string? TableId);

/// <summary>One column. <see cref="Fields"/> is only ever populated for a <c>RECORD</c>/<c>STRUCT</c>
/// column (self-referencing, matching BigQuery's own nested schema shape) -- this connector's own
/// schema map never emits one, but a table read back from BigQuery may still carry one for a column
/// this connector does not otherwise touch.</summary>
internal sealed record BqFieldSchema(string? Name, string? Type, string? Mode, string? Precision, string? Scale, BqFieldSchema[]? Fields);

internal sealed record BqTableSchema(BqFieldSchema[]? Fields);

internal sealed record BqTable(BqTableReference? TableReference, BqTableSchema? Schema, string? Type, string? ExpirationTime);

/// <summary>The body of a <c>tables.patch</c> call. <see cref="ExpirationTime"/> is milliseconds
/// since the epoch as a string -- BigQuery's own wire encoding for every millisecond timestamp,
/// chosen so a JavaScript client never loses precision to <c>double</c> rounding.</summary>
internal sealed record BqTablePatch(string? ExpirationTime);

internal sealed record BqJobReference(string? ProjectId, string? JobId, string? Location);

internal sealed record BqErrorProto(string? Reason, string? Message, string? Location);

internal sealed record BqJobStatus(string? State, BqErrorProto? ErrorResult, BqErrorProto[]? Errors);

internal sealed record BqLoadConfig(string? SourceFormat, BqTableSchema? Schema, BqTableReference? DestinationTable, string? WriteDisposition, string? CreateDisposition);

internal sealed record BqQueryConfig(string? Query, bool? UseLegacySql, BqTableReference? DestinationTable, string? WriteDisposition, string? CreateDisposition);

/// <summary>Exactly one of <see cref="Load"/>/<see cref="Query"/> is set per job -- this connector
/// never submits a job carrying both, but the wire shape allows either to be absent independently
/// (BigQuery's own <c>jobConfiguration</c> has several more variants this connector never sends).</summary>
internal sealed record BqJobConfiguration(BqLoadConfig? Load, BqQueryConfig? Query);

/// <summary>The wire shape (this file); <c>Jobs/BqJob.cs</c> reopens this same partial record with
/// the submission/polling operations (<c>SubmitAndWaitAsync</c>, <c>NewJobId</c>) over it -- a
/// same-named static class cannot coexist with this record in one namespace, so the operational
/// surface is added to the type itself rather than declared as a sibling.</summary>
internal sealed partial record BqJob(BqJobReference? JobReference, BqJobConfiguration? Configuration, BqJobStatus? Status);

internal sealed record BqDatasetListEntry(string? Id);

internal sealed record BqDatasetList(BqDatasetListEntry[]? Datasets);

internal sealed record BqErrorBody(int? Code, string? Message, BqErrorProto[]? Errors);

/// <summary>The envelope every BigQuery REST error response body is wrapped in: <c>{"error": {...}}</c>.</summary>
internal sealed record BqErrorEnvelope(BqErrorBody? Error);
