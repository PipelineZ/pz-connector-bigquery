namespace Pz.Connector.BigQuery;

/// <summary>Builds the load job that lands one spool file into the staging table (spec §7.1 step
/// 2): NDJSON in, appended (the staging table is created empty immediately before, so every load
/// job append-only), and refusing to create the table itself (<c>CREATE_NEVER</c> -- a missing
/// staging table at this point is a bug in the commit sequence, not something a load job should
/// paper over).</summary>
internal static class BqLoadJob
{
    public static BqJob Build(string jobId, string? location, TableRef staging, BqTableSchema schema) =>
        new(
            new BqJobReference(staging.Project, jobId, location),
            new BqJobConfiguration(
                Load: new BqLoadConfig(
                    SourceFormat: "NEWLINE_DELIMITED_JSON",
                    Schema: schema,
                    DestinationTable: new BqTableReference(staging.Project, staging.Dataset, staging.Table),
                    WriteDisposition: "WRITE_APPEND",
                    CreateDisposition: "CREATE_NEVER"),
                Query: null),
            Status: null);
}
