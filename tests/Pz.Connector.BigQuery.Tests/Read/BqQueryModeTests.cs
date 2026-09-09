using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>Query-mode reads (<c>query:</c> instead of <c>entity:</c>): a query job materializes the
/// result into <c>staging_dataset</c>, then the table-mode path (schema resolution, pushdown, Storage
/// Read API) takes over unchanged. Every spec pins a single stream for the same reason
/// <see cref="BqSourceBehaviorTests"/> does -- this emulator's Storage Read API refuses any other
/// stream count.</summary>
[Collection("bigquery")]
[Trait("Category", "Docker")]
public sealed class BqQueryModeTests
{
    private readonly BqFixture _bq;

    public BqQueryModeTests(BqFixture bq)
    {
        _bq = bq;
        DockerFacts.SkipUnlessDocker();
    }

    private async Task<ISource> OpenAsync(string? stagingDataset = BqFixture.Dataset)
    {
        ISourceConnector connector = new BqConnector();
        return await connector.OpenAsync(new ConnectorConfig(_bq.ConnectionConfig(stagingDataset)), CancellationToken.None);
    }

    private static DatasetSpec QuerySpec(string sql, Dictionary<string, object?>? extra = null)
    {
        var options = extra is null ? new Dictionary<string, object?>() : new Dictionary<string, object?>(extra);
        options["query"] = sql;
        options.TryAdd("streams", 1);
        return new DatasetSpec("bigquery", "qmode", options);
    }

    private static async Task<List<RecordBatch>> DrainAsync(IEnumerable<IDatasetPartition> partitions)
    {
        var batches = new List<RecordBatch>();
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batches.Add(batch);
            }
        }

        return batches;
    }

    private static List<long> LongColumn(IReadOnlyList<RecordBatch> batches, int index)
    {
        var result = new List<long>();
        foreach (var batch in batches)
        {
            var column = (Int64Array)batch.Column(index);
            for (var row = 0; row < batch.Length; row++)
            {
                result.Add(column.GetValue(row)!.Value);
            }
        }

        return result;
    }

    [SkippableFact]
    public async Task Query_materializes_into_a_staged_table_with_the_projected_schema_and_rows()
    {
        var table = BqFixture.NewName("qbase");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]""");
        await _bq.InsertRowsAsync(table, Enumerable.Range(0, 20).Select(i => $$"""{"id":{{i}},"name":"n{{i}}"}"""));

        var spec = QuerySpec($"select id, name from {BqFixture.Dataset}.{table} where id < 10");

        await using var source = await OpenAsync();
        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        Assert.Equal(["id", "name"], schema.Schema.FieldsList.Select(f => f.Name));

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var batches = await DrainAsync(partitions);
        Assert.Equal(10, batches.Sum(b => b.Length));
    }

    [SkippableFact]
    public async Task Pushdown_hints_apply_on_top_of_the_materialized_query_result()
    {
        var table = BqFixture.NewName("qpush");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]""");
        await _bq.InsertRowsAsync(table, Enumerable.Range(0, 20).Select(i => $$"""{"id":{{i}},"name":"n{{i}}"}"""));

        var spec = QuerySpec($"select id, name from {BqFixture.Dataset}.{table} where id < 10");

        await using var source = await OpenAsync();
        await source.GetSchemaAsync(spec, CancellationToken.None);

        var hints = new ReadHints(Columns: ["id"], PredicateSql: "id >= 5");
        var partitions = await source.PlanReadAsync(spec, hints, CancellationToken.None);
        var batches = await DrainAsync(partitions);

        Assert.Equal(["id"], batches[0].Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(Enumerable.Range(5, 5).Select(i => (long)i), LongColumn(batches, 0).Order());
    }

    [SkippableFact]
    public async Task GetSchema_then_PlanRead_share_one_materialize_job_and_dispose_drops_the_table()
    {
        var table = BqFixture.NewName("qonejob");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");
        await _bq.InsertRowsAsync(table, ["""{"id":1}"""]);

        var spec = QuerySpec($"select id from {BqFixture.Dataset}.{table}");
        var source = await OpenAsync();
        try
        {
            await source.GetSchemaAsync(spec, CancellationToken.None);
            await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);

            var midSession = await _bq.ListTablesAsync(BqFixture.Dataset);
            Assert.Single(midSession, t => t.StartsWith("pz_query_", StringComparison.Ordinal));
        }
        finally
        {
            await source.DisposeAsync();
        }

        var afterDispose = await _bq.ListTablesAsync(BqFixture.Dataset);
        Assert.DoesNotContain(afterDispose, t => t.StartsWith("pz_query_", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Materialized_table_carries_an_expiration()
    {
        var table = BqFixture.NewName("qexpiry");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");
        await _bq.InsertRowsAsync(table, ["""{"id":1}"""]);

        var spec = QuerySpec($"select id from {BqFixture.Dataset}.{table}");
        await using var source = await OpenAsync();
        await source.GetSchemaAsync(spec, CancellationToken.None);

        var tables = await _bq.ListTablesAsync(BqFixture.Dataset);
        var materialized = Assert.Single(tables, t => t.StartsWith("pz_query_", StringComparison.Ordinal));

        var meta = await _bq.GetTableAsync(materialized);
        Assert.NotNull(meta);
        Assert.True(meta!.Value.TryGetProperty("expirationTime", out var expiration));
        Assert.False(string.IsNullOrEmpty(expiration.GetString()));
    }

    [SkippableFact]
    public async Task Trailing_semicolon_in_the_query_is_tolerated()
    {
        var table = BqFixture.NewName("qsemi");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");
        await _bq.InsertRowsAsync(table, ["""{"id":1}""", """{"id":2}"""]);

        var spec = QuerySpec($"select id from {BqFixture.Dataset}.{table};   ");
        await using var source = await OpenAsync();
        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        Assert.Equal(["id"], schema.Schema.FieldsList.Select(f => f.Name));
    }

    [SkippableFact]
    public async Task Query_without_staging_dataset_fails_with_PZBQ0105()
    {
        var spec = QuerySpec("select 1 as id");
        await using var source = await OpenAsync(stagingDataset: null);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => source.GetSchemaAsync(spec, CancellationToken.None).AsTask());

        Assert.Contains("PZBQ0105", ex.Message, StringComparison.Ordinal);
    }

    // Real BigQuery's job errorResult classifies through BqJob.SubmitAndWaitAsync into PZBQ0407,
    // naming "materialize query" -- proven deterministically against a synthetic response in
    // BqQueryMaterializerTests.A_failed_job_reports_PZBQ0407_naming_the_materialize_step, since this
    // emulator (BqFixture.Image, 0.8.1) cannot reproduce that path at all: confirmed directly against
    // its REST API, a broken query's jobs.insert response carries a genuine errorResult (reason
    // "jobInternalError"), but the very next jobs.get for that same jobId already reports
    // {"state":"DONE"} with no error whatsoever -- the emulator drops it once the job is re-fetched.
    // SubmitAndWaitAsync deliberately never trusts the insert response's own status (a real BigQuery
    // jobs.insert essentially never answers already DONE), so it always re-polls and never observes
    // the failure here either: the materialize job looks like it succeeded, and the next step
    // (tables.patch on the destination that CREATE_IF_NEEDED never actually created, because the
    // query itself never ran) is what this emulator classifies -- HTTP 404 -> PZBQ0406, naming the
    // destination table. What IS provable here is that a real failure still surfaces as a classified
    // PzConnectorException naming the table this emulator's quirk left dangling, not a hang or an
    // unclassified crash.
    [SkippableFact]
    public async Task Broken_query_is_reported_as_a_classified_failure_this_emulator_actually_produces()
    {
        var spec = QuerySpec("select this is not valid sql {{{");
        await using var source = await OpenAsync();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => source.GetSchemaAsync(spec, CancellationToken.None).AsTask());

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0406", ex.Message, StringComparison.Ordinal);
        Assert.Contains("pz_query_", ex.Message, StringComparison.Ordinal);
    }
}
