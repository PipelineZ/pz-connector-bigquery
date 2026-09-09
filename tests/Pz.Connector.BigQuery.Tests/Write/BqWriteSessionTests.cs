using System.Globalization;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>Offline coverage (no docker, no network) for two <see cref="BqWriteSession.CommitAsync"/>
/// details a live emulator cannot show without a race: <c>CommitAsync</c> creates the staging table,
/// runs the target job, and deletes staging all inside one method (its own <c>finally</c>), so by the
/// time a docker fact could inspect the emulator's table list the staging table is already gone --
/// unlike the read-side query materializer, whose cleanup is a separate, later <c>DisposeAsync</c>
/// call (see <c>BqQueryModeTests.GetSchema_then_PlanRead_share_one_materialize_job_and_dispose_drops_the_table</c>).
/// A <see cref="FakeHandler"/> records the exact request BigQuery would receive instead: this proves
/// the staging table's <c>expirationTime</c> and which dataset it targets without needing to catch it
/// mid-flight. Every write here has zero rows (no batches), so the load-job leg never runs and the
/// only REST calls are staging create, target get, target job, and (tolerated 404, unrouted) staging
/// delete.</summary>
public sealed class BqWriteSessionTests
{
    private static readonly Schema Schema = new(
        [new Field("id", Int64Type.Default, nullable: true), new Field("name", StringType.Default, nullable: true)], null);

    private static BqConnectionConfig Config(TableRef? stagingDataset = null) => new(
        "p", BqAuthKind.None, null, null, null, stagingDataset, BqConnectionConfig.DefaultRestBase, null, false, BqRedactor.None);

    private static BqRestClient RestClient(FakeHandler handler, BqConnectionConfig cfg) =>
        new(new HttpClient(handler), cfg, BqRedactor.None, NullLogger.Instance, _ => Task.FromResult<string?>(null));

    private static void RouteTargetMissingThenCreatingJobDone(
        FakeHandler handler, string project, string dataset, string table, string stagingDataset)
    {
        handler.Add(HttpMethod.Post, $"/bigquery/v2/projects/{project}/datasets/{stagingDataset}/tables", 200, "{}");
        // tables.get on the target: no route registered falls through to FakeHandler's own 404,
        // which GetTableAsync already tolerates as "missing" -- explicit here only for readability.
        handler.Add(HttpMethod.Get, $"/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}", 404,
            """{"error":{"code":404,"message":"not found","errors":[{"reason":"notFound"}]}}""");
        handler.Add(HttpMethod.Post, $"/bigquery/v2/projects/{project}/jobs", 200,
            """{"jobReference":{"projectId":"p","jobId":"commit1"},"status":{"state":"DONE"}}""");
    }

    private static async Task<FakeHandler.Recorded> CommitAndCaptureStagingInsertAsync(
        BqConnectionConfig cfg, TableRef target, TimeProvider time)
    {
        var handler = new FakeHandler();
        var stagingDatasetSegment = cfg.StagingDataset?.Dataset ?? target.Dataset;
        RouteTargetMissingThenCreatingJobDone(handler, target.Project, target.Dataset, target.Table, stagingDatasetSegment);

        var rest = RestClient(handler, cfg);
        var sink = new BqSink(cfg, rest, BqRedactor.None, NullLogger.Instance, time);
        var spec = new OutputSpec("bigquery", $"{target.Dataset}.{target.Table}", "append", "fail_on_change", new Dictionary<string, object?>());

        await using var session = await sink.BeginWriteAsync(spec, Schema, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        var insert = Assert.Single(handler.Requests, r =>
            r.Method == HttpMethod.Post
            && r.Url.AbsolutePath == $"/bigquery/v2/projects/{target.Project}/datasets/{stagingDatasetSegment}/tables");
        return insert;
    }

    [Fact]
    public async Task Staging_table_carries_expirationTime_six_hours_out_and_the_mapped_schema()
    {
        var time = new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var target = new TableRef("p", "e2e", "t1");

        var insert = await CommitAndCaptureStagingInsertAsync(Config(), target, time);

        using var body = JsonDocument.Parse(insert.Body);
        var expectedExpiry = (time.GetUtcNow() + TimeSpan.FromHours(6)).ToUnixTimeMilliseconds()
            .ToString(CultureInfo.InvariantCulture);
        Assert.Equal(expectedExpiry, body.RootElement.GetProperty("expirationTime").GetString());

        var fields = body.RootElement.GetProperty("schema").GetProperty("fields").EnumerateArray().ToList();
        Assert.Equal(["id", "name"], fields.Select(f => f.GetProperty("name").GetString()));
        Assert.Equal("INTEGER", fields[0].GetProperty("type").GetString());
        Assert.Equal("STRING", fields[1].GetProperty("type").GetString());
    }

    [Fact]
    public async Task Staging_table_name_is_pz_load_prefixed()
    {
        var target = new TableRef("p", "e2e", "t1");

        var insert = await CommitAndCaptureStagingInsertAsync(Config(), target, TimeProvider.System);

        using var body = JsonDocument.Parse(insert.Body);
        var tableId = body.RootElement.GetProperty("tableReference").GetProperty("tableId").GetString();
        Assert.Matches("^pz_load_[0-9a-f]{32}$", tableId);
    }

    [Fact]
    public async Task Staging_lands_in_the_configured_staging_dataset_not_the_targets_own()
    {
        var target = new TableRef("p", "e2e", "t1");
        var cfg = Config(stagingDataset: new TableRef("p", "other", ""));

        var insert = await CommitAndCaptureStagingInsertAsync(cfg, target, TimeProvider.System);

        Assert.Equal("/bigquery/v2/projects/p/datasets/other/tables", insert.Url.AbsolutePath);
    }

    [Fact]
    public async Task Staging_falls_back_to_the_targets_own_dataset_when_no_staging_dataset_is_configured()
    {
        var target = new TableRef("p", "e2e", "t1");

        var insert = await CommitAndCaptureStagingInsertAsync(Config(), target, TimeProvider.System);

        Assert.Equal("/bigquery/v2/projects/p/datasets/e2e/tables", insert.Url.AbsolutePath);
    }

    /// <summary>Merge against an ALREADY-EXISTING target dispatches to <see cref="BqSql.Merge"/>'s DML
    /// -- proved here by capturing the exact submitted job SQL and comparing it against
    /// <see cref="BqSql.Merge"/> computed independently with the same staging table (its name is
    /// read back off the staging <c>tables.insert</c> request, since <c>CommitAsync</c> mints it from
    /// a fresh <see cref="Guid"/> the test cannot predict). This is what
    /// <c>BqSinkBehaviorTests.Merge_into_an_existing_target_fails_with_the_emulators_own_job_error</c>
    /// cannot prove against the live emulator, whose MERGE parser refuses to execute this SQL shape at
    /// all -- FakeHandler never executes GoogleSQL, only records and answers, so the dispatch is
    /// provable independently of whether this particular emulator build can run it.</summary>
    [Fact]
    public async Task Merge_into_an_existing_target_dispatches_BqSql_Merge_with_the_sessions_staging_table()
    {
        var cfg = Config();
        var target = new TableRef("p", "e2e", "t1");
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/datasets/e2e/tables", 200, "{}");
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/e2e/tables/t1", 200,
            """{"tableReference":{"projectId":"p","datasetId":"e2e","tableId":"t1"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"NULLABLE"},{"name":"name","type":"STRING","mode":"NULLABLE"}]}}""");
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/jobs", 200,
            """{"jobReference":{"projectId":"p","jobId":"commit1"},"status":{"state":"DONE"}}""");

        var rest = RestClient(handler, cfg);
        var sink = new BqSink(cfg, rest, BqRedactor.None, NullLogger.Instance, TimeProvider.System);
        var spec = new OutputSpec("bigquery", "e2e.t1", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };

        await using var session = await sink.BeginWriteAsync(spec, Schema, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        var stagingInsert = handler.Requests.Single(r =>
            r.Method == HttpMethod.Post && r.Url.AbsolutePath == "/bigquery/v2/projects/p/datasets/e2e/tables");
        using var stagingBody = JsonDocument.Parse(stagingInsert.Body);
        var stagingTableId = stagingBody.RootElement.GetProperty("tableReference").GetProperty("tableId").GetString()!;
        var staging = new TableRef("p", "e2e", stagingTableId);

        var jobInsert = handler.Requests.Single(r => r.Method == HttpMethod.Post && r.Url.AbsolutePath == "/bigquery/v2/projects/p/jobs");
        using var jobBody = JsonDocument.Parse(jobInsert.Body);
        var submittedSql = jobBody.RootElement.GetProperty("configuration").GetProperty("query").GetProperty("query").GetString();

        Assert.Equal(BqSql.Merge(target, staging, ["id", "name"], ["id"]), submittedSql);
    }

    /// <summary>Replace sends <c>writeDisposition: WRITE_TRUNCATE</c> (and the exact
    /// <see cref="BqSql.Select"/> query text) on its query-destination job -- proved here since the
    /// live emulator does not actually HONOR that disposition (see <c>BqSinkBehaviorTests</c>' class
    /// doc), so a docker fact asserting on the target's final row count cannot tell a correct
    /// disposition from an ignored one.</summary>
    [Fact]
    public async Task Replace_sends_WRITE_TRUNCATE_on_the_target_job()
    {
        var target = new TableRef("p", "e2e", "t1");
        var (jobInsert, staging) = await CommitAndCaptureCommitJobAsync("replace", target);

        using var body = JsonDocument.Parse(jobInsert.Body);
        var query = body.RootElement.GetProperty("configuration").GetProperty("query");
        Assert.Equal("WRITE_TRUNCATE", query.GetProperty("writeDisposition").GetString());
        Assert.Equal("CREATE_IF_NEEDED", query.GetProperty("createDisposition").GetString());
        Assert.Equal(BqSql.Select(staging, ["id", "name"]), query.GetProperty("query").GetString());
    }

    private static async Task<(FakeHandler.Recorded JobInsert, TableRef Staging)> CommitAndCaptureCommitJobAsync(string mode, TableRef target)
    {
        var cfg = Config();
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, $"/bigquery/v2/projects/{target.Project}/datasets/{target.Dataset}/tables", 200, "{}");
        handler.Add(HttpMethod.Get, $"/bigquery/v2/projects/{target.Project}/datasets/{target.Dataset}/tables/{target.Table}", 404,
            """{"error":{"code":404,"message":"not found","errors":[{"reason":"notFound"}]}}""");
        handler.Add(HttpMethod.Post, $"/bigquery/v2/projects/{target.Project}/jobs", 200,
            """{"jobReference":{"projectId":"p","jobId":"commit1"},"status":{"state":"DONE"}}""");

        var rest = RestClient(handler, cfg);
        var sink = new BqSink(cfg, rest, BqRedactor.None, NullLogger.Instance, TimeProvider.System);
        var spec = new OutputSpec("bigquery", $"{target.Dataset}.{target.Table}", mode, "fail_on_change", new Dictionary<string, object?>());

        await using var session = await sink.BeginWriteAsync(spec, Schema, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        var stagingInsert = handler.Requests.Single(r =>
            r.Method == HttpMethod.Post && r.Url.AbsolutePath == $"/bigquery/v2/projects/{target.Project}/datasets/{target.Dataset}/tables");
        using var stagingBody = JsonDocument.Parse(stagingInsert.Body);
        var stagingTableId = stagingBody.RootElement.GetProperty("tableReference").GetProperty("tableId").GetString()!;
        var staging = new TableRef(target.Project, target.Dataset, stagingTableId);

        var jobInsert = handler.Requests.Single(r => r.Method == HttpMethod.Post && r.Url.AbsolutePath == $"/bigquery/v2/projects/{target.Project}/jobs");
        return (jobInsert, staging);
    }

    private static BqWriteSession NewSession(
        BqConnectionConfig cfg, BqRestClient rest, TableRef target, string mode, string[] keys, BqSpool spool)
    {
        var withSequence = string.Equals(mode, "merge", StringComparison.Ordinal);
        var stagingSchema = BqSchemaMap.ToBigQuery(Schema, withSequence, target.Table);
        var targetWantedSchema = BqSchemaMap.ToBigQuery(Schema, withSequence: false, target.Table);
        var spec = new OutputSpec("bigquery", $"{target.Dataset}.{target.Table}", mode, "fail_on_change", new Dictionary<string, object?>())
        {
            Keys = keys,
        };
        return new BqWriteSession(
            cfg, rest, BqRedactor.None, NullLogger.Instance, TimeProvider.System, target, spec, Schema,
            ["id", "name"], stagingSchema, targetWantedSchema, withSequence, spool);
    }

    /// <summary>A <see cref="BqSpool"/> whose <see cref="Delete"/> always throws -- <see cref="BqSpool"/>
    /// is not sealed specifically to allow this seam. Used to prove the Critical/Important cleanup
    /// findings deterministically: no permission tricks, no platform-specific filesystem behavior,
    /// no race against a real directory delete.</summary>
    private sealed class ThrowingDeleteSpool(string dir) : BqSpool(dir)
    {
        public override void Delete() => throw new IOException("simulated spool deletion failure");
    }

    /// <summary>Critical finding: a spool-deletion failure in <c>CommitAsync</c>'s <c>finally</c>
    /// must never discard an already-successful commit's <see cref="WriteResult"/> -- the engine
    /// would otherwise retry a write that already landed (duplicate rows in <c>append</c> mode).</summary>
    [Fact]
    public async Task Commit_still_returns_WriteResult_when_spool_deletion_fails()
    {
        var cfg = Config();
        var target = new TableRef("p", "e2e", "t1");
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/datasets/e2e/tables", 200, "{}");
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/e2e/tables/t1", 404,
            """{"error":{"code":404,"message":"not found","errors":[{"reason":"notFound"}]}}""");
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/jobs", 200,
            """{"jobReference":{"projectId":"p","jobId":"commit1"},"status":{"state":"DONE"}}""");
        var rest = RestClient(handler, cfg);
        var session = NewSession(cfg, rest, target, "append", [], new ThrowingDeleteSpool(TempDir()));

        var result = await session.CommitAsync(CancellationToken.None);

        Assert.Equal(0, result.RowsWritten);
    }

    /// <summary>Critical finding, other half: when the target job itself fails, that PRIMARY
    /// exception -- not the spool's own I/O exception -- must be what the caller sees. A cleanup
    /// failure masking the real cause would misreport an invalid-SQL failure as an unrelated
    /// filesystem error.</summary>
    [Fact]
    public async Task Commit_surfaces_the_primary_exception_not_a_spool_deletion_failure()
    {
        var cfg = Config();
        var target = new TableRef("p", "e2e", "t1");
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/datasets/e2e/tables", 200, "{}");
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/e2e/tables/t1", 404,
            """{"error":{"code":404,"message":"not found","errors":[{"reason":"notFound"}]}}""");
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/jobs", 200,
            """{"jobReference":{"projectId":"p","jobId":"commit1"},"status":{"state":"DONE","errorResult":{"reason":"invalidQuery","message":"bad sql"}}}""");
        var rest = RestClient(handler, cfg);
        var session = NewSession(cfg, rest, target, "append", [], new ThrowingDeleteSpool(TempDir()));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await session.CommitAsync(CancellationToken.None));

        Assert.Contains("PZBQ0407", ex.Message);
    }

    /// <summary>Important finding: an aborted session's spool-deletion failure must not propagate
    /// out of <see cref="ISinkWriteSession.AbortAsync"/> either.</summary>
    [Fact]
    public async Task Abort_does_not_throw_when_spool_deletion_fails()
    {
        var cfg = Config();
        var target = new TableRef("p", "e2e", "t1");
        var rest = RestClient(new FakeHandler(), cfg);
        var session = NewSession(cfg, rest, target, "append", [], new ThrowingDeleteSpool(TempDir()));

        var ex = await Record.ExceptionAsync(async () => await session.AbortAsync(CancellationToken.None));

        Assert.Null(ex);
    }

    /// <summary>Important finding: <see cref="ISinkWriteSession.AbortAsync"/> must close the spool's
    /// open file handle before deleting the directory -- deleting first (the pre-fix order) leaves a
    /// dangling handle on every platform and hard-fails on Windows. Proved directly against
    /// <see cref="BqSpool.IsOpen"/> rather than the delete's success/failure, since deleting a
    /// directory out from under an open handle does not actually fail on Linux -- only Windows would
    /// have caught the old ordering.</summary>
    [Fact]
    public async Task Abort_closes_the_open_spool_file_before_deleting_the_directory()
    {
        var cfg = Config();
        var target = new TableRef("p", "e2e", "t1");
        var rest = RestClient(new FakeHandler(), cfg);
        var spool = new BqSpool(TempDir());
        var session = NewSession(cfg, rest, target, "append", [], spool);
        var batch = OneRowBatch();
        await session.WriteBatchAsync(batch, CancellationToken.None);
        batch.Dispose();
        Assert.True(spool.IsOpen);

        await session.AbortAsync(CancellationToken.None);

        Assert.False(spool.IsOpen);
    }

    [Fact]
    public async Task Evolve_is_refused_at_BeginWriteAsync_with_no_network_call()
    {
        var cfg = Config();
        var handler = new FakeHandler();
        var rest = RestClient(handler, cfg);
        var sink = new BqSink(cfg, rest, BqRedactor.None, NullLogger.Instance, TimeProvider.System);
        var spec = new OutputSpec("bigquery", "e2e.t1", "append", "evolve", new Dictionary<string, object?>());

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, Schema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0305", ex.Message);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task BeginWriteAsync_bad_write_mode_is_PZBQ0306_with_no_network_call()
    {
        var cfg = Config();
        var handler = new FakeHandler();
        var rest = RestClient(handler, cfg);
        var sink = new BqSink(cfg, rest, BqRedactor.None, NullLogger.Instance, TimeProvider.System);
        var spec = new OutputSpec("bigquery", "e2e.t1", "upsert", "fail_on_change", new Dictionary<string, object?>());

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, Schema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0306", ex.Message);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task BeginWriteAsync_merge_with_no_keys_is_PZBQ0301_with_no_network_call()
    {
        var cfg = Config();
        var handler = new FakeHandler();
        var rest = RestClient(handler, cfg);
        var sink = new BqSink(cfg, rest, BqRedactor.None, NullLogger.Instance, TimeProvider.System);
        var spec = new OutputSpec("bigquery", "e2e.t1", "merge", "fail_on_change", new Dictionary<string, object?>()); // Keys defaults to []

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, Schema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0301", ex.Message);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task BeginWriteAsync_reserved_seq_column_is_PZBQ0302_with_no_network_call()
    {
        var cfg = Config();
        var handler = new FakeHandler();
        var rest = RestClient(handler, cfg);
        var sink = new BqSink(cfg, rest, BqRedactor.None, NullLogger.Instance, TimeProvider.System);
        var schemaWithSeq = new Schema(
            [new Field("id", Int64Type.Default, true), new Field(BqSchemaMap.SequenceColumn, Int64Type.Default, true)], null);
        var spec = new OutputSpec("bigquery", "e2e.t1", "append", "fail_on_change", new Dictionary<string, object?>());

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, schemaWithSeq, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0302", ex.Message);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task BeginWriteAsync_bad_output_option_is_PZBQ0307_with_no_network_call()
    {
        var cfg = Config();
        var handler = new FakeHandler();
        var rest = RestClient(handler, cfg);
        var sink = new BqSink(cfg, rest, BqRedactor.None, NullLogger.Instance, TimeProvider.System);
        var spec = new OutputSpec("bigquery", "e2e.t1", "append", "fail_on_change", new Dictionary<string, object?> { ["bogus"] = "x" });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, Schema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0307", ex.Message);
        Assert.Empty(handler.Requests);
    }

    /// <summary>Important finding: <c>Spool_rolling_lands_every_row_across_multiple_load_jobs</c>
    /// (docker) only counts rows, which would pass unchanged even if rolling never happened (one
    /// file, one load job, same 15 rows). This proves the actual mechanism: two batches with
    /// <c>RollBytes = 1</c> roll into two separate spool files, so <c>CommitAsync</c> submits two
    /// resumable-upload initiations (one load job each) and still runs the target statement exactly
    /// once.</summary>
    [Fact]
    public async Task Spool_rolling_at_RollBytes_1_produces_one_load_job_per_batch_and_the_target_job_once()
    {
        var cfg = Config();
        var target = new TableRef("p", "e2e", "t1");
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/datasets/e2e/tables", 200, "{}");
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/e2e/tables/t1", 404,
            """{"error":{"code":404,"message":"not found","errors":[{"reason":"notFound"}]}}""");
        handler.Add(HttpMethod.Post, "/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", 200, "ignored",
            new Dictionary<string, string> { ["Location"] = "http://fake-upload/session" });
        handler.Add(HttpMethod.Put, "/session", 200,
            """{"jobReference":{"projectId":"p","jobId":"load1"},"status":{"state":"DONE"}}""");
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/jobs", 200,
            """{"jobReference":{"projectId":"p","jobId":"commit1"},"status":{"state":"DONE"}}""");

        var rest = RestClient(handler, cfg);
        var sink = new BqSink(cfg, rest, BqRedactor.None, NullLogger.Instance, TimeProvider.System, spoolRollBytes: 1);
        var spec = new OutputSpec("bigquery", "e2e.t1", "append", "fail_on_change", new Dictionary<string, object?>());

        await using var session = await sink.BeginWriteAsync(spec, Schema, CancellationToken.None);
        var batch1 = OneRowBatch();
        await session.WriteBatchAsync(batch1, CancellationToken.None);
        batch1.Dispose();
        var batch2 = OneRowBatch();
        await session.WriteBatchAsync(batch2, CancellationToken.None);
        batch2.Dispose();

        await session.CommitAsync(CancellationToken.None);

        var loadInitiations = handler.Requests.Count(r =>
            r.Method == HttpMethod.Post && r.Url.AbsolutePath == "/upload/bigquery/v2/projects/p/jobs");
        Assert.Equal(2, loadInitiations);

        var targetJobs = handler.Requests.Count(r => r.Method == HttpMethod.Post && r.Url.AbsolutePath == "/bigquery/v2/projects/p/jobs");
        Assert.Equal(1, targetJobs);
    }

    private static RecordBatch OneRowBatch()
    {
        var ids = new Int64Array.Builder().Append(1).Build();
        var names = new StringArray.Builder().Append("a").Build();
        return new RecordBatch(Schema, [ids, names], 1);
    }

    private static string TempDir() => Path.Combine(Path.GetTempPath(), "pz-bigquery-tests", Guid.NewGuid().ToString("N"));
}
