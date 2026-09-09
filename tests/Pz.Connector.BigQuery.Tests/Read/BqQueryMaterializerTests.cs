using System.Net;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>Offline coverage (no docker, no network) for <see cref="BqQueryMaterializer"/> failure
/// classification, caching, and single-flight submission -- deterministic where a live emulator's own
/// scheduling and job timing cannot pin down the exact request sequence a synthetic response can. A
/// PATCH route is never registered for the <see cref="FakeHandler"/>-backed facts below: every one of
/// those fails before or at the query job itself, so <c>tables.patch</c> is never reached and an
/// unmatched request there would only mask a bug in routing, not exercise one.</summary>
public sealed class BqQueryMaterializerTests
{
    private const string Project = "p";

    private static BqConnectionConfig Config() => new(
        Project, BqAuthKind.None, null, null, null, new TableRef(Project, "stg", ""),
        BqConnectionConfig.DefaultRestBase, null, false, BqRedactor.None);

    private static BqRestClient RestClient(HttpMessageHandler handler) =>
        new(new HttpClient(handler), Config(), BqRedactor.None, NullLogger.Instance, _ => Task.FromResult<string?>(null));

    private static BqQueryMaterializer Materializer(HttpMessageHandler handler) =>
        new(RestClient(handler), Config(), new FakeTimeProvider(), NullLogger.Instance);

    private static void AddFailedJob(FakeHandler handler, string jobId, string reason, string message)
    {
        var jobRef = "\"jobReference\":{\"projectId\":\"p\",\"jobId\":\"" + jobId + "\"}";
        handler.Add(HttpMethod.Post, $"/bigquery/v2/projects/{Project}/jobs", 200, "{" + jobRef + "}");
        handler.Add(HttpMethod.Get, $"/bigquery/v2/projects/{Project}/jobs/{jobId}", 200,
            "{" + jobRef + ",\"status\":{\"state\":\"DONE\",\"errorResult\":{\"reason\":\"" + reason
            + "\",\"message\":\"" + message + "\"}}}");
    }

    [Fact]
    public async Task A_failed_job_reports_PZBQ0407_naming_the_materialize_step()
    {
        var handler = new FakeHandler();
        AddFailedJob(handler, "job1", "invalidQuery", "Syntax error");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => Materializer(handler).MaterializeAsync("select 1", CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0407", ex.Message, StringComparison.Ordinal);
        Assert.Contains("materialize query", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_faulted_materialization_is_not_cached_forever()
    {
        var handler = new FakeHandler();
        AddFailedJob(handler, "job1", "invalidQuery", "boom");
        var materializer = Materializer(handler);

        await Assert.ThrowsAsync<PzConnectorException>(() => materializer.MaterializeAsync("select 1", CancellationToken.None));

        // A distinct second job id for the exact same query text -- if the first failure were cached
        // forever, this route would never be requested and the retry would replay the stale failure
        // (job1) instead of submitting a fresh job.
        AddFailedJob(handler, "job2", "invalidQuery", "boom again");

        await Assert.ThrowsAsync<PzConnectorException>(() => materializer.MaterializeAsync("select 1", CancellationToken.None));

        Assert.Contains(handler.Requests, r => r.Method == HttpMethod.Get && r.Url.AbsolutePath.EndsWith("/job2", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Trailing_semicolon_and_whitespace_are_stripped_before_wrapping()
    {
        var handler = new FakeHandler();
        AddFailedJob(handler, "job1", "invalidQuery", "irrelevant -- only the outgoing request matters here");
        var materializer = Materializer(handler);

        await Assert.ThrowsAsync<PzConnectorException>(
            () => materializer.MaterializeAsync("  select 1;  \n", CancellationToken.None));

        var insert = Assert.Single(handler.Requests, r => r.Method == HttpMethod.Post);
        Assert.Contains("\"query\":\"select * from (\\nselect 1\\n)\"", insert.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_failed_expiration_patch_still_lets_DisposeAsync_drop_the_destination_table()
    {
        var handler = new PatchFailingHandler();
        var materializer = Materializer(handler);

        await Assert.ThrowsAsync<PzConnectorException>(() => materializer.MaterializeAsync("select 1", CancellationToken.None));

        // The table was created by the (successful) query job even though the patch that follows it
        // failed -- DisposeAsync must still drop it, not skip it because the materialization as a
        // whole never completed successfully.
        await materializer.DisposeAsync();

        var deleted = Assert.Single(handler.DeletedPaths);
        Assert.StartsWith("/bigquery/v2/projects/p/datasets/stg/tables/pz_query_", deleted, StringComparison.Ordinal);
    }

    /// <summary>Always lands the query job as DONE (no error) so the destination table is genuinely
    /// created, then fails every <c>tables.patch</c> -- proving Important-1's fix independently of
    /// the job-failure path the other facts in this file already cover. <c>DELETE</c> is recorded and
    /// answered successfully so the test can assert on it without a fixed path to route by (the
    /// destination's name is a random GUID the materializer mints itself).</summary>
    private sealed class PatchFailingHandler : HttpMessageHandler
    {
        public List<string> DeletedPaths { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.Method == HttpMethod.Post)
            {
                return Task.FromResult(Json(HttpStatusCode.OK,
                    "{\"jobReference\":{\"projectId\":\"p\",\"jobId\":\"job1\"},\"status\":{\"state\":\"DONE\"}}"));
            }

            if (request.Method == HttpMethod.Patch)
            {
                return Task.FromResult(Json((HttpStatusCode)500,
                    """{"error":{"code":500,"message":"boom","errors":[{"reason":"backendError"}]}}"""));
            }

            if (request.Method == HttpMethod.Delete)
            {
                DeletedPaths.Add(request.RequestUri!.AbsolutePath);
                return Task.FromResult(Json(HttpStatusCode.OK, "{}"));
            }

            return Task.FromResult(Json(HttpStatusCode.NotFound, "{}"));
        }

        private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
            new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
    }

    [Fact]
    public async Task Cancelling_one_callers_token_does_not_cancel_or_fault_a_sibling_callers_wait()
    {
        var handler = new GatedInsertHandler();
        var materializer = Materializer(handler);
        using var cts1 = new CancellationTokenSource();

        var t1 = materializer.MaterializeAsync("select 1", cts1.Token);
        var t2 = materializer.MaterializeAsync("select 1", CancellationToken.None);

        // Both calls are in flight, sharing the one materialization, before either token is touched.
        await handler.FirstRequestArrived;
        Assert.False(t1.IsCompleted);
        Assert.False(t2.IsCompleted);

        cts1.Cancel();

        // t1's own token fired -- it must observe cancellation, but the shared materialization
        // neither this call started nor is entitled to cancel keeps running underneath for t2.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => t1);
        Assert.False(t2.IsCompleted);

        handler.ReleaseGate();
        var table = await t2;

        Assert.Equal(1, handler.InsertCalls);
        Assert.Equal("stg", table.Dataset);
    }

    [Fact]
    public async Task Concurrent_materializations_of_the_same_query_submit_exactly_one_job()
    {
        var handler = new GatedInsertHandler();
        var materializer = Materializer(handler);

        // A shared, not-yet-completed start signal both racers await first: neither can call
        // MaterializeAsync until this fires, so completing it (after both are already queued on the
        // thread pool) gives them the closest thing to a simultaneous start no sleep-based scheme
        // could -- the scenario the ConcurrentDictionary.GetOrAdd contract warns about (its value
        // factory may run for more than one caller racing on the same absent key).
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task<TableRef> RaceAsync()
        {
            await start.Task.ConfigureAwait(false);
            return await materializer.MaterializeAsync("select 1", CancellationToken.None).ConfigureAwait(false);
        }

        var t1 = Task.Run(RaceAsync);
        var t2 = Task.Run(RaceAsync);
        start.SetResult();

        // Both calls are provably in flight together before either can finish: the handler has
        // received its first (and, if the single-flight guarantee holds, only) request, yet neither
        // task has completed -- whichever call actually reached the handler is blocked on the gate
        // below, and the other is blocked waiting on that same shared materialization.
        await handler.FirstRequestArrived;
        Assert.False(t1.IsCompleted);
        Assert.False(t2.IsCompleted);

        handler.ReleaseGate();
        var results = await Task.WhenAll(t1, t2);

        Assert.Equal(1, handler.InsertCalls);
        Assert.Equal(results[0], results[1]);
    }

    /// <summary>Counts <c>jobs.insert</c> (POST) calls and holds every one of them open on a shared
    /// gate until <see cref="ReleaseGate"/> is called -- lets a test observe "a request has arrived"
    /// and "nothing has completed yet" as two separate, non-racy facts instead of inferring either
    /// from timing. Any non-POST request (the winning call's <c>tables.patch</c>, once released)
    /// succeeds immediately and unconditionally: its destination table name is randomly minted by
    /// the materializer, so this test has no fixed path to route on and does not need one.</summary>
    private sealed class GatedInsertHandler : HttpMessageHandler
    {
        private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _firstRequestArrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _insertCalls;

        public int InsertCalls => _insertCalls;

        public Task FirstRequestArrived => _firstRequestArrived.Task;

        public void ReleaseGate() => _gate.TrySetResult();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.Method != HttpMethod.Post)
            {
                return Json("{}");
            }

            var n = Interlocked.Increment(ref _insertCalls);
            _firstRequestArrived.TrySetResult();
            await _gate.Task.ConfigureAwait(false);
            return Json("{\"jobReference\":{\"projectId\":\"p\",\"jobId\":\"job" + n + "\"},\"status\":{\"state\":\"DONE\"}}");
        }

        private static HttpResponseMessage Json(string body) =>
            new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
    }
}
