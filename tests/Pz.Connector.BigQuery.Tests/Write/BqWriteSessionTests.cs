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

    /// <summary>Replace sends <c>writeDisposition: WRITE_TRUNCATE</c> on its query-destination job --
    /// proved here since the live emulator does not actually HONOR that disposition (see
    /// <c>BqSinkBehaviorTests</c>' class doc), so a docker fact asserting on the target's final row
    /// count cannot tell a correct disposition from an ignored one.</summary>
    [Fact]
    public async Task Replace_sends_WRITE_TRUNCATE_on_the_target_job()
    {
        var target = new TableRef("p", "e2e", "t1");
        var insert = await CommitAndCaptureCommitJobAsync("replace", target);

        using var body = JsonDocument.Parse(insert.Body);
        var query = body.RootElement.GetProperty("configuration").GetProperty("query");
        Assert.Equal("WRITE_TRUNCATE", query.GetProperty("writeDisposition").GetString());
        Assert.Equal("CREATE_IF_NEEDED", query.GetProperty("createDisposition").GetString());
    }

    private static async Task<FakeHandler.Recorded> CommitAndCaptureCommitJobAsync(string mode, TableRef target)
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

        return handler.Requests.Single(r => r.Method == HttpMethod.Post && r.Url.AbsolutePath == $"/bigquery/v2/projects/{target.Project}/jobs");
    }
}
