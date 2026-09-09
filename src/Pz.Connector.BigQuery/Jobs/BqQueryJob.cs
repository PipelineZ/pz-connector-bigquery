namespace Pz.Connector.BigQuery;

/// <summary>Builds the one query job that touches a sink's target table (spec §7.1 step 4, append
/// and replace variants -- merge uses a DML statement instead, still through this same builder).
/// <paramref name="destination"/>/<paramref name="writeDisposition"/>/<paramref name="createDisposition"/>
/// are set only when given: a plain query with no destination (the merge statement, or any ad hoc
/// SQL this connector never targets at a table) needs none of the three. When
/// <paramref name="destination"/> is null there is no table to read a project id from either, so
/// <c>jobReference.projectId</c> is left unset -- BigQuery resolves it from the
/// <c>jobs.insert</c> URL's own project in that case.</summary>
internal static class BqQueryJob
{
    public static BqJob Build(
        string jobId, string? location, string sql, TableRef? destination, string? writeDisposition, string? createDisposition) =>
        new(
            new BqJobReference(destination?.Project, jobId, location),
            new BqJobConfiguration(
                Load: null,
                Query: new BqQueryConfig(
                    Query: sql,
                    UseLegacySql: false,
                    DestinationTable: destination is { } d ? new BqTableReference(d.Project, d.Dataset, d.Table) : null,
                    WriteDisposition: writeDisposition,
                    CreateDisposition: createDisposition)),
            Status: null);
}
