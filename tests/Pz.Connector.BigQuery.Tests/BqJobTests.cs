using System.Net;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqJobTests
{
    [Fact]
    public void NewJobId_is_prefixed_with_pz_and_the_purpose()
    {
        var id = BqJob.NewJobId("load_staging");

        Assert.StartsWith("pz_load_staging_", id);
        Assert.Matches("^pz_load_staging_[0-9a-f]{32}$", id);
    }

    [Fact]
    public void NewJobId_is_unique_per_call()
    {
        Assert.NotEqual(BqJob.NewJobId("x"), BqJob.NewJobId("x"));
    }

    [Fact]
    public async Task SubmitAndWaitAsync_polls_with_the_backoff_schedule_and_returns_the_done_job()
    {
        var time = new FakeTimeProvider();
        var handler = new SequenceHandler(
            insertBody: JobJson("job1", "PENDING"),
            getBodies: [JobJson("job1", "RUNNING"), JobJson("job1", "RUNNING"), JobJson("job1", "DONE")]);
        var client = Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);

        var resultTask = BqJob.SubmitAndWaitAsync(client, "p", job, "loading into staging", time, NullLogger.Instance, CancellationToken.None);

        // call #1: no delay before it -- awaits the handler's own signal (a real gate, not a
        // wall-clock poll) that the response has been served.
        await handler.WaitForGetAsync(1);
        Assert.Equal(1, handler.InsertCalls);

        // The 250 ms delay before call #2: prove BOTH edges, not just "enough time eventually
        // passed" -- 249 ms must NOT be enough (a too-short delay in the production code would
        // still pass a test that only checked the positive edge), and the next 1 ms must be.
        time.Advance(TimeSpan.FromMilliseconds(249));
        await LetPendingContinuationsRunAsync();
        Assert.Equal(1, handler.GetCalls);

        time.Advance(TimeSpan.FromMilliseconds(1));
        await handler.WaitForGetAsync(2);

        // The 500 ms delay before call #3: same two-edge proof.
        time.Advance(TimeSpan.FromMilliseconds(499));
        await LetPendingContinuationsRunAsync();
        Assert.Equal(2, handler.GetCalls);

        time.Advance(TimeSpan.FromMilliseconds(1));
        await handler.WaitForGetAsync(3);

        var result = await resultTask;

        Assert.Equal("DONE", result.Status?.State);
        Assert.Equal(3, handler.GetCalls);
    }

    [Fact]
    public async Task SubmitAndWaitAsync_done_with_error_result_throws_classified_with_the_purpose()
    {
        var time = new FakeTimeProvider();
        var handler = new SequenceHandler(
            insertBody: JobJson("job1", "PENDING"),
            getBodies: [JobJson("job1", "DONE", errorReason: "invalidQuery", errorMessage: "Syntax error")]);
        var client = Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => BqJob.SubmitAndWaitAsync(client, "p", job, "compiling pipeline", time, NullLogger.Instance, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0407", ex.Message);
        Assert.Contains("compiling pipeline", ex.Message);
    }

    [Fact]
    public async Task SubmitAndWaitAsync_falls_back_to_the_submitted_jobs_location_when_the_insert_response_omits_it()
    {
        // The insert response below carries no jobReference.location at all; the submitted job
        // carries "EU". Every jobs.get call must still be addressed to ?location=EU -- a FakeHandler
        // route keyed on that exact query string is what makes a wrong (or missing) location show up
        // as a 404-turned-PZBQ0406 instead of silently passing.
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/jobs", 200, JobJson("job1", "PENDING"));
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/jobs/job1?location=EU", 200, JobJson("job1", "DONE"));
        var client = Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", "EU"), null, null);
        var time = new FakeTimeProvider();

        var result = await BqJob.SubmitAndWaitAsync(client, "p", job, "loading", time, NullLogger.Instance, CancellationToken.None);

        Assert.Equal("DONE", result.Status?.State);
    }

    [Fact]
    public void BqLoadJob_Build_shapes_an_NDJSON_append_load()
    {
        var schema = new BqTableSchema([new BqFieldSchema("c", "STRING", null, null, null, null)]);

        var job = BqLoadJob.Build("job1", "US", new TableRef("p", "staging", "pz_load_x"), schema);

        Assert.Equal("job1", job.JobReference?.JobId);
        Assert.Equal("US", job.JobReference?.Location);
        Assert.Equal("NEWLINE_DELIMITED_JSON", job.Configuration?.Load?.SourceFormat);
        Assert.Equal("WRITE_APPEND", job.Configuration?.Load?.WriteDisposition);
        Assert.Equal("CREATE_NEVER", job.Configuration?.Load?.CreateDisposition);
        Assert.Same(schema, job.Configuration?.Load?.Schema);
        Assert.Equal("pz_load_x", job.Configuration?.Load?.DestinationTable?.TableId);
        Assert.Null(job.Configuration?.Query);
    }

    [Fact]
    public void BqQueryJob_Build_sets_UseLegacySql_false_and_omits_destination_when_none_given()
    {
        var job = BqQueryJob.Build("job2", null, "select 1", destination: null, writeDisposition: null, createDisposition: null);

        Assert.Equal("select 1", job.Configuration?.Query?.Query);
        Assert.Equal(false, job.Configuration?.Query?.UseLegacySql);
        Assert.Null(job.Configuration?.Query?.DestinationTable);
        Assert.Null(job.Configuration?.Load);
    }

    [Fact]
    public void BqQueryJob_Build_carries_the_destination_and_dispositions_when_given()
    {
        var job = BqQueryJob.Build("job3", "EU", "select * from x", new TableRef("p", "d", "t"), "WRITE_TRUNCATE", "CREATE_IF_NEEDED");

        Assert.Equal("t", job.Configuration?.Query?.DestinationTable?.TableId);
        Assert.Equal("WRITE_TRUNCATE", job.Configuration?.Query?.WriteDisposition);
        Assert.Equal("CREATE_IF_NEEDED", job.Configuration?.Query?.CreateDisposition);
        Assert.Equal("EU", job.JobReference?.Location);
    }

    private static BqRestClient Client(HttpMessageHandler handler)
    {
        var errors = new List<string>();
        var config = BqConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["project"] = "p",
            ["auth"] = "none",
            ["endpoint"] = "http://fake/",
        }), errors)!;

        return new BqRestClient(new HttpClient(handler), config, null, BqRedactor.None, NullLogger.Instance);
    }

    /// <summary>Gives every continuation already queued on the thread pool a chance to run, without
    /// waiting on real wall-clock time -- used only to prove a NEGATIVE ("nothing happened yet"),
    /// never to wait for something that is expected to happen (that always goes through a real
    /// signal, e.g. <see cref="SequenceHandler.WaitForGetAsync"/>).</summary>
    private static async Task LetPendingContinuationsRunAsync()
    {
        for (var i = 0; i < 50; i++)
        {
            await Task.Yield();
        }
    }

    private static string JobJson(string jobId, string state, string? errorReason = null, string? errorMessage = null)
    {
        var sb = new StringBuilder();
        sb.Append("{\"jobReference\":{\"projectId\":\"p\",\"jobId\":\"").Append(jobId).Append("\"},");
        sb.Append("\"status\":{\"state\":\"").Append(state).Append('"');
        if (errorReason is not null)
        {
            sb.Append(",\"errorResult\":{\"reason\":\"").Append(errorReason).Append('"');
            if (errorMessage is not null)
            {
                sb.Append(",\"message\":\"").Append(errorMessage).Append('"');
            }

            sb.Append('}');
        }

        sb.Append("}}");
        return sb.ToString();
    }

    /// <summary>A minimal call-counting handler for the polling tests: <c>jobs.insert</c> (POST)
    /// always answers with <paramref name="insertBody"/>; each <c>jobs.get</c> (GET) answers with the
    /// next body in <paramref name="getBodies"/> (the last one repeats once exhausted).
    /// <see cref="WaitForGetAsync"/> is the deterministic alternative to polling <see cref="GetCalls"/>
    /// on a wall-clock timer: it completes exactly when the handler has served the Nth GET, however
    /// many scheduling hops that took.</summary>
    private sealed class SequenceHandler(string insertBody, IReadOnlyList<string> getBodies) : HttpMessageHandler
    {
        private readonly Queue<string> _getBodies = new(getBodies);
        private readonly List<TaskCompletionSource> _getServed = [];
        private readonly Lock _gate = new();

        public int InsertCalls { get; private set; }

        public int GetCalls { get; private set; }

        /// <summary>Completes once the Nth GET call has been served. May be called before that call
        /// happens (the wait is registered up front and signaled later) or after (it is already
        /// signaled, so the await returns immediately).</summary>
        public Task WaitForGetAsync(int n)
        {
            lock (_gate)
            {
                return TcsFor(n).Task;
            }
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.Method == HttpMethod.Post)
            {
                InsertCalls++;
                return Task.FromResult(Respond(insertBody));
            }

            lock (_gate)
            {
                GetCalls++;
                TcsFor(GetCalls).TrySetResult();
            }

            var body = _getBodies.Count > 1 ? _getBodies.Dequeue() : _getBodies.Peek();
            return Task.FromResult(Respond(body));
        }

        // Must be called with _gate held.
        private TaskCompletionSource TcsFor(int n)
        {
            while (_getServed.Count < n)
            {
                _getServed.Add(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            }

            return _getServed[n - 1];
        }

        private static HttpResponseMessage Respond(string body) =>
            new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
    }
}
