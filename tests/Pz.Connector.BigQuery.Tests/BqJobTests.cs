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
            getBodies: [JobJson("job1", "RUNNING"), JobJson("job1", "RUNNING"), JobJson("job1", "DONE")],
            time: time);
        var client = BqRestClientTests_Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);

        var resultTask = BqJob.SubmitAndWaitAsync(client, "p", job, "loading into staging", time, NullLogger.Instance, CancellationToken.None);

        await WaitUntilAsync(() => handler.GetCalls == 1);
        Assert.Equal(1, handler.InsertCalls);

        time.Advance(TimeSpan.FromMilliseconds(250));
        await WaitUntilAsync(() => handler.GetCalls == 2);

        time.Advance(TimeSpan.FromMilliseconds(500));
        await WaitUntilAsync(() => handler.GetCalls == 3);

        var result = await resultTask;

        Assert.Equal("DONE", result.Status?.State);
        Assert.Equal(3, handler.GetCalls);
        Assert.Equal(new[] { TimeSpan.FromMilliseconds(250), TimeSpan.FromMilliseconds(500) }, handler.ObservedDelays);
    }

    [Fact]
    public async Task SubmitAndWaitAsync_done_with_error_result_throws_classified_with_the_purpose()
    {
        var time = new FakeTimeProvider();
        var handler = new SequenceHandler(
            insertBody: JobJson("job1", "PENDING"),
            getBodies: [JobJson("job1", "DONE", errorReason: "invalidQuery", errorMessage: "Syntax error")],
            time: time);
        var client = BqRestClientTests_Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => BqJob.SubmitAndWaitAsync(client, "p", job, "compiling pipeline", time, NullLogger.Instance, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0407", ex.Message);
        Assert.Contains("compiling pipeline", ex.Message);
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

    private static BqRestClient BqRestClientTests_Client(HttpMessageHandler handler)
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

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        // FakeTimeProvider.Advance runs due timer callbacks inline, but the awaiting continuation
        // (the next loop iteration's GetJobAsync call) still needs a thread-pool hop to resume --
        // this polls for that, bounded, rather than asserting on a race.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("condition did not become true in time");
            }

            await Task.Delay(5);
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
    /// next body in <paramref name="getBodies"/> (the last one repeats once exhausted). Records the
    /// gap between successive GET calls on <paramref name="time"/> -- the same <see cref="TimeProvider"/>
    /// the polling loop's own <c>Task.Delay</c> advances against -- so the test asserts the backoff
    /// schedule the production code actually awaited, not a real-wall-clock approximation of it.</summary>
    private sealed class SequenceHandler(string insertBody, IReadOnlyList<string> getBodies, TimeProvider time) : HttpMessageHandler
    {
        private readonly Queue<string> _getBodies = new(getBodies);
        private DateTimeOffset? _lastGetAt;

        public int InsertCalls { get; private set; }

        public int GetCalls { get; private set; }

        public List<TimeSpan> ObservedDelays { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.Method == HttpMethod.Post)
            {
                InsertCalls++;
                return Task.FromResult(Respond(insertBody));
            }

            GetCalls++;
            var now = time.GetUtcNow();
            if (_lastGetAt is { } last)
            {
                ObservedDelays.Add(now - last);
            }

            _lastGetAt = now;

            var body = _getBodies.Count > 1 ? _getBodies.Dequeue() : _getBodies.Peek();
            return Task.FromResult(Respond(body));
        }

        private static HttpResponseMessage Respond(string body) =>
            new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
    }
}
