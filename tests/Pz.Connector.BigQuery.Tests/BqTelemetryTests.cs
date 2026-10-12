using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqTelemetryTests
{
    private const string InsertPath = "/bigquery/v2/projects/p/jobs";

    [Fact]
    public async Task Job_statistics_map_to_cost_remote_ms_and_extras()
    {
        var acc = new ConnectorStatsAccumulator();
        var body = """
            {"jobReference":{"projectId":"p","jobId":"job1"},"status":{"state":"DONE"},
             "statistics":{"startTime":"1700000000000","endTime":"1700000002500","totalBytesProcessed":"4096","totalSlotMs":"120",
                           "query":{"totalBytesBilled":"10485760","totalBytesProcessed":"4096"}}}
            """;

        await RunJobAsync(acc, 200, body);

        var s = acc.Snapshot();
        Assert.Equal(10485760, s.CostUnits);
        Assert.Equal("bytes_billed", s.CostUnit);
        Assert.Equal(2500, s.RemoteMs);
        Assert.Contains(new KeyValuePair<string, double>("bigquery.slot_ms", 120), s.Extras);
        Assert.Contains(new KeyValuePair<string, double>("bigquery.bytes_processed", 4096), s.Extras);
        Assert.Equal(1, s.Requests);
    }

    [Fact]
    public async Task Job_without_statistics_reports_only_the_request()
    {
        var acc = new ConnectorStatsAccumulator();

        await RunJobAsync(acc, 200, """{"jobReference":{"projectId":"p","jobId":"job1"},"status":{"state":"DONE"}}""");

        var s = acc.Snapshot();
        Assert.Equal(1, s.Requests);
        Assert.Null(s.CostUnit);
        Assert.Equal(0, s.RemoteMs);
        Assert.Empty(s.Extras);
    }

    [Fact]
    public void Zero_billed_and_malformed_statistics_report_nothing_and_never_throw()
    {
        var acc = new ConnectorStatsAccumulator();

        BqTelemetry.ReportJob(
            new BqJobStatistics("x", "-5", "abc", null, new BqQueryStatistics("0", null)), acc);

        Assert.True(acc.Snapshot().IsEmpty);
    }

    [Fact]
    public void Rate_limit_exceeded_on_a_403_and_on_a_job_error_counts_a_throttle()
    {
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            BqErrors.FromRest(403, "rateLimitExceeded", "slow", null, BqRedactor.None, "ctx");
            BqErrors.FromJobError("rateLimitExceeded", "slow", BqRedactor.None, "loading");
        }

        Assert.Equal(2, acc.Snapshot().Throttled);
    }

    [Fact]
    public void A_429_is_left_to_the_http_handler_and_other_errors_are_not_throttles()
    {
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            BqErrors.FromRest(429, "rateLimitExceeded", "slow", null, BqRedactor.None, "ctx");
            BqErrors.FromRest(403, "quotaExceeded", "no", null, BqRedactor.None, "ctx");
            BqErrors.FromRest(404, null, "gone", null, BqRedactor.None, "ctx");
        }

        Assert.Equal(0, acc.Snapshot().Throttled);
    }

    [Fact]
    public void Grpc_resource_exhausted_counts_a_throttle()
    {
        var acc = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(acc))
        {
            BqErrors.FromRpc(
                new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.ResourceExhausted, "quota")),
                BqRedactor.None, "reading");
            BqErrors.FromRpc(
                new Grpc.Core.RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.Unavailable, "down")),
                BqRedactor.None, "reading");
        }

        Assert.Equal(1, acc.Snapshot().Throttled);
    }

    [Fact]
    public void Connector_declares_ReportsStats()
    {
        Assert.True(new BqConnector().Capabilities.HasFlag(ConnectorCapabilities.ReportsStats));
    }

    private static async Task RunJobAsync(ConnectorStatsAccumulator acc, int status, string body)
    {
        using var source = new ActivitySource("bq-test");
        var remote = new RemoteCalls(source);
        var fake = new FakeHandler();
        fake.Add(HttpMethod.Post, InsertPath, status, body);
        var config = BqConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["project"] = "p",
            ["auth"] = "none",
            ["endpoint"] = "http://fake/",
        }), [])!;
        var client = new BqRestClient(new HttpClient(remote.HttpHandler(fake)), config, null, BqRedactor.None, NullLogger.Instance);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);

        using (ConnectorStats.Use(acc))
        {
            await BqJob.SubmitAndWaitAsync(client, "p", job, "loading", new FakeTimeProvider(), NullLogger.Instance, CancellationToken.None);
        }
    }
}
