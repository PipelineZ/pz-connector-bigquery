using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>Offline coverage (no docker, no network) for <see cref="BqQueryMaterializer"/> failure
/// classification and caching. The PZBQ0407-naming-the-purpose path this covers cannot be exercised
/// against a live emulator: <c>BqQueryModeTests.Broken_query_is_reported_as_a_classified_failure_this_
/// emulator_actually_produces</c> documents why (<c>BqFixture.Image</c>, 0.8.1, drops a job's
/// <c>errorResult</c> the moment it is re-fetched via <c>jobs.get</c>) -- a synthetic response is the
/// only way to prove this classification actually fires. A PATCH route is never registered below:
/// every fact here fails before or at the query job itself, so <c>tables.patch</c> is never reached
/// and an unmatched request there would only mask a bug in routing, not exercise one.</summary>
public sealed class BqQueryMaterializerTests
{
    private const string Project = "p";

    private static BqConnectionConfig Config() => new(
        Project, BqAuthKind.None, null, null, null, new TableRef(Project, "stg", ""),
        BqConnectionConfig.DefaultRestBase, null, false, BqRedactor.None);

    private static BqRestClient RestClient(FakeHandler handler) =>
        new(new HttpClient(handler), Config(), BqRedactor.None, NullLogger.Instance, _ => Task.FromResult<string?>(null));

    private static BqQueryMaterializer Materializer(FakeHandler handler) =>
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
}
