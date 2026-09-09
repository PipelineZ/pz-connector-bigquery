using System.Globalization;
using System.Net.Http.Headers;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Google.Apis.Auth.OAuth2;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>Facts against real BigQuery, proving what <c>BqFixture</c>'s emulator
/// (goccy/bigquery-emulator 0.8.1) cannot: the exact Arrow type real BigQuery's own NUMERIC,
/// BIGNUMERIC, DATETIME, TIME, GEOGRAPHY, JSON, ARRAY and TIMESTAMP columns declare, a genuine
/// WRITE_TRUNCATE replace, a real view's Storage Read API refusal (PZBQ0204), and how many read
/// streams the service actually grants. Every fact skips unless both <c>PZ_BIGQUERY_PROJECT</c> and
/// <c>GOOGLE_APPLICATION_CREDENTIALS</c> are set -- no live BigQuery project is reachable from this
/// machine, so running the suite with them unset is expected to SKIP every fact here, never fail.
///
/// <para>Each fact owns its own dataset (created and torn down inside the fact, after the skip
/// check), rather than a shared fixture -- a shared <c>IAsyncLifetime</c> would create the dataset
/// before the skip check ever runs, turning "vars unset" into a network failure instead of a
/// skip.</para></summary>
[Trait("Category", "LiveBigQuery")]
public sealed class LiveBigQueryFacts
{
    /// <summary>One live project's credential, REST client, and admin <see cref="HttpClient"/>,
    /// scoped to one throwaway dataset. <see cref="Rest"/> is used for job submission through
    /// <see cref="BqJob.SubmitAndWaitAsync"/> (DDL/DML); <see cref="Http"/> plus a fetched bearer
    /// token is used for the dataset admin calls and the query-job row read-back <see cref="Rest"/>
    /// has no method for.</summary>
    private sealed class LiveContext(string project, string dataset, GoogleCredential? credential, BqRestClient rest, HttpClient http)
    {
        public string Project { get; } = project;

        public string Dataset { get; } = dataset;

        public GoogleCredential? Credential { get; } = credential;

        public BqRestClient Rest { get; } = rest;

        public HttpClient Http { get; } = http;
    }

    private static void SkipUnlessLive() =>
        Skip.If(
            string.IsNullOrEmpty(Environment.GetEnvironmentVariable("PZ_BIGQUERY_PROJECT"))
            || string.IsNullOrEmpty(Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS")),
            "set PZ_BIGQUERY_PROJECT and GOOGLE_APPLICATION_CREDENTIALS to run against real BigQuery");

    private static string NewDatasetName()
    {
        var bytes = new byte[6];
        Random.Shared.NextBytes(bytes);
        return $"pz_live_{Convert.ToHexString(bytes).ToLowerInvariant()}";
    }

    /// <summary>The connection config every connector open in this file uses: no <c>endpoint</c>, so
    /// <see cref="BqConnectionConfig.Parse"/> defaults <c>RestBase</c> to real BigQuery, and
    /// <c>auth: adc</c> so credentials come from <c>GOOGLE_APPLICATION_CREDENTIALS</c>.
    /// <paramref name="stagingDataset"/> is set only for a query-mode read (Fact 2's read-back needs
    /// none -- it queries through the admin REST path, not a query-mode source).</summary>
    private static Dictionary<string, object?> ConnectionConfig(string project, string? stagingDataset = null)
    {
        var values = new Dictionary<string, object?> { ["project"] = project, ["auth"] = "adc" };
        if (stagingDataset is not null)
        {
            values["staging_dataset"] = stagingDataset;
        }

        return values;
    }

    private static Field FieldOf(Schema schema, string name) =>
        schema.GetFieldByName(name) ?? throw new InvalidOperationException($"schema has no field '{name}'");

    private static async Task<LiveContext> CreateLiveContextAsync()
    {
        var project = Environment.GetEnvironmentVariable("PZ_BIGQUERY_PROJECT")
            ?? throw new InvalidOperationException("PZ_BIGQUERY_PROJECT is required (checked by SkipUnlessLive)");
        var dataset = NewDatasetName();

        var errors = new List<string>();
        var connection = BqConnectionConfig.Parse(new ConnectorConfig(ConnectionConfig(project)), errors)
            ?? throw new InvalidOperationException($"bad live connection config: {string.Join("; ", errors)}");
        var credential = BqAuth.Create(connection);
        var http = new HttpClient();
        try
        {
            var rest = new BqRestClient(http, connection, credential, connection.Redactor, NullLogger.Instance);
            var ctx = new LiveContext(project, dataset, credential, rest, http);

            // No `location` is passed on this create or on any job submission -- every DDL/DML/query
            // job in this file relies on the project's dataset-default location.
            await SendAsync(ctx, HttpMethod.Post, $"bigquery/v2/projects/{project}/datasets",
                "{\"datasetReference\":{\"projectId\":\"" + project + "\",\"datasetId\":\"" + dataset + "\"}}").ConfigureAwait(false);
            return ctx;
        }
        catch
        {
            // No LiveContext escaped this method, so nothing else owns http yet -- without this, a
            // failed dataset create would leak it (DeleteLiveDatasetAsync never gets a ctx to run
            // against).
            http.Dispose();
            throw;
        }
    }

    private static async Task DeleteLiveDatasetAsync(LiveContext ctx)
    {
        try
        {
            await SendAsync(ctx, HttpMethod.Delete,
                $"bigquery/v2/projects/{ctx.Project}/datasets/{ctx.Dataset}?deleteContents=true", null).ConfigureAwait(false);
        }
        finally
        {
            ctx.Http.Dispose();
        }
    }

    /// <summary>Creates a live context, runs <paramref name="body"/> against it, and always attempts
    /// the dataset delete afterward -- but a failed delete never replaces the fact's own assertion
    /// failure. If <paramref name="body"/> throws, that exception propagates unwrapped (with its
    /// original stack trace) regardless of whether cleanup also fails; a cleanup failure only ever
    /// surfaces on its own, when <paramref name="body"/> itself succeeded, since a dataset that
    /// failed to delete after an otherwise-passing fact is a real leak worth reporting. Plain
    /// <c>try { ... } finally { await Delete(...); }</c> in each fact would not have this property:
    /// an exception thrown from a <c>finally</c> block replaces whatever was already propagating
    /// from the <c>try</c>, so a correlated failure (the same connector bug breaking both the
    /// assertion and the cleanup delete) would report the wrong one.</summary>
    private static async Task RunLiveFactAsync(Func<LiveContext, Task> body)
    {
        var ctx = await CreateLiveContextAsync().ConfigureAwait(false);
        Exception? cleanupError = null;
        try
        {
            await body(ctx).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                await DeleteLiveDatasetAsync(ctx).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                cleanupError = ex;
            }
        }

        if (cleanupError is not null)
        {
            ExceptionDispatchInfo.Capture(cleanupError).Throw();
        }
    }

    private static async Task<string> SendAsync(LiveContext ctx, HttpMethod method, string path, string? body)
    {
        var token = await BqAuth.AccessTokenAsync(ctx.Credential, CancellationToken.None).ConfigureAwait(false);
        using var request = new HttpRequestMessage(method, new Uri(BqConnectionConfig.DefaultRestBase, path));
        if (body is not null)
        {
            request.Content = new StringContent(body, Encoding.UTF8, "application/json");
        }

        if (token is not null)
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        }

        using var response = await ctx.Http.SendAsync(request).ConfigureAwait(false);
        var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"{method} {path} -> {(int)response.StatusCode}: {text}");
        }

        return text;
    }

    /// <summary>Runs a DDL or DML statement (CREATE TABLE, CREATE VIEW, INSERT, CREATE TABLE ... AS
    /// SELECT) to completion via <see cref="BqJob.SubmitAndWaitAsync"/> -- the same submit/poll/
    /// classify path the connector itself uses, so a live failure here comes back as the same
    /// classified <see cref="PzConnectorException"/> a caller would see.</summary>
    private static Task ExecuteDdlAsync(LiveContext ctx, string sql)
    {
        var job = new BqJob(
            JobReference: new BqJobReference(ctx.Project, BqJob.NewJobId("live"), null),
            Configuration: new BqJobConfiguration(
                Load: null,
                Query: new BqQueryConfig(Query: sql, UseLegacySql: false, DestinationTable: null, WriteDisposition: null, CreateDisposition: null)),
            Status: null);
        return BqJob.SubmitAndWaitAsync(ctx.Rest, ctx.Project, job, "live ddl", TimeProvider.System, NullLogger.Instance, CancellationToken.None);
    }

    /// <summary>Runs <paramref name="sql"/> via <c>jobs.query</c>, polling <c>jobs.getQueryResults</c>
    /// (and following every <c>pageToken</c>) until the result is complete and fully paged in --
    /// mirrors <c>BqFixture.QueryAsync</c>'s shape against the admin REST path instead of the
    /// emulator, reusing <see cref="BqFixture.AssembleRows"/> for the row assembly itself.</summary>
    private static async Task<List<JsonElement>> QueryLiveAsync(LiveContext ctx, string sql)
    {
        var body = JsonSerializer.Serialize(new { query = sql, useLegacySql = false });
        var text = await SendAsync(ctx, HttpMethod.Post, $"bigquery/v2/projects/{ctx.Project}/queries", body).ConfigureAwait(false);

        var page = ParseQueryPage(text);
        var jobId = GetJobId(page) ?? throw new InvalidOperationException("query response carried no jobReference.jobId");
        var location = GetLocation(page);

        for (var attempt = 0; !IsComplete(page); attempt++)
        {
            if (attempt >= 100)
            {
                throw new TimeoutException($"live query job {jobId} did not complete in time");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(200)).ConfigureAwait(false);
            var pollText = await SendAsync(ctx, HttpMethod.Get, QueryResultsPath(ctx.Project, jobId, location, null), null).ConfigureAwait(false);
            page = ParseQueryPage(pollText);
        }

        var pages = new List<JsonElement> { page };
        var pageToken = GetPageToken(page);
        while (!string.IsNullOrEmpty(pageToken))
        {
            var pageText = await SendAsync(ctx, HttpMethod.Get, QueryResultsPath(ctx.Project, jobId, location, pageToken), null)
                .ConfigureAwait(false);
            var nextPage = ParseQueryPage(pageText);
            pages.Add(nextPage);
            pageToken = GetPageToken(nextPage);
        }

        return BqFixture.AssembleRows(pages);
    }

    private static async Task<List<(long Id, string? Name)>> ReadBackIdNameAsync(LiveContext ctx, string table)
    {
        var rows = await QueryLiveAsync(ctx, $"select id, name from `{ctx.Project}.{ctx.Dataset}.{table}` order by id")
            .ConfigureAwait(false);
        return rows
            .Select(r => (long.Parse(r.GetProperty("id").GetString()!, CultureInfo.InvariantCulture), r.GetProperty("name").GetString()))
            .ToList();
    }

    private static JsonElement ParseQueryPage(string text)
    {
        using var doc = JsonDocument.Parse(text);
        var root = doc.RootElement.Clone();
        if (root.TryGetProperty("errors", out var errors) && errors.GetArrayLength() > 0)
        {
            throw new InvalidOperationException($"live query reported errors: {errors}");
        }

        return root;
    }

    private static bool IsComplete(JsonElement page) =>
        !page.TryGetProperty("jobComplete", out var complete) || complete.GetBoolean();

    private static string? GetJobId(JsonElement page) =>
        page.TryGetProperty("jobReference", out var jobRef) && jobRef.TryGetProperty("jobId", out var jobId) ? jobId.GetString() : null;

    private static string? GetLocation(JsonElement page) =>
        page.TryGetProperty("jobReference", out var jobRef) && jobRef.TryGetProperty("location", out var loc) ? loc.GetString() : null;

    private static string? GetPageToken(JsonElement page) =>
        page.TryGetProperty("pageToken", out var token) ? token.GetString() : null;

    private static string QueryResultsPath(string project, string jobId, string? location, string? pageToken)
    {
        var path = $"bigquery/v2/projects/{project}/queries/{jobId}";
        var query = new List<string>();
        if (!string.IsNullOrEmpty(location))
        {
            query.Add("location=" + Uri.EscapeDataString(location));
        }

        if (!string.IsNullOrEmpty(pageToken))
        {
            query.Add("pageToken=" + Uri.EscapeDataString(pageToken));
        }

        return query.Count == 0 ? path : path + "?" + string.Join("&", query);
    }

    private static readonly Schema IdName = new(
        [new Field("id", Int64Type.Default, nullable: true), new Field("name", StringType.Default, nullable: true)], null);

    private static RecordBatch IdNameBatch(params (long Id, string Name)[] rows)
    {
        var builder = new ArrowBatchBuilder(IdName);
        foreach (var (id, name) in rows)
        {
            builder.AppendRow([(object?)id, name]);
        }

        return builder.Flush()!;
    }

    private static async Task WriteAsync(ISink sink, OutputSpec spec, IReadOnlyList<RecordBatch> batches)
    {
        await using var session = await sink.BeginWriteAsync(spec, IdName, CancellationToken.None);
        foreach (var batch in batches)
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
            batch.Dispose();
        }

        await session.CommitAsync(CancellationToken.None);
    }

    [SkippableFact]
    public async Task Wide_type_matrix_declares_real_BigQuerys_own_Arrow_types()
    {
        SkipUnlessLive();
        await RunLiveFactAsync(async ctx =>
        {
            const string table = "types";
            await ExecuteDdlAsync(ctx, $"""
                create table `{ctx.Project}.{ctx.Dataset}.{table}` (
                  num NUMERIC, bignum BIGNUMERIC, dt DATETIME, t TIME, geo GEOGRAPHY, j JSON,
                  arr ARRAY<INT64>, ts TIMESTAMP
                )
                """).ConfigureAwait(false);

            ISourceConnector connector = new BqConnector();
            await using var source = await connector.OpenAsync(new ConnectorConfig(ConnectionConfig(ctx.Project)), CancellationToken.None);
            var spec = new DatasetSpec("bigquery", $"{ctx.Dataset}.{table}", new Dictionary<string, object?> { ["streams"] = 1 });
            var schema = await source.GetSchemaAsync(spec, CancellationToken.None);

            var numeric = Assert.IsType<Decimal128Type>(FieldOf(schema.Schema, "num").DataType);
            Assert.Equal(38, numeric.Precision);
            Assert.Equal(9, numeric.Scale);

            var bignumeric = Assert.IsType<Decimal256Type>(FieldOf(schema.Schema, "bignum").DataType);
            Assert.Equal(76, bignumeric.Precision);
            Assert.Equal(38, bignumeric.Scale);

            var datetime = Assert.IsType<TimestampType>(FieldOf(schema.Schema, "dt").DataType);
            Assert.Equal(TimeUnit.Microsecond, datetime.Unit);
            Assert.Null(datetime.Timezone);

            var time = Assert.IsType<Time64Type>(FieldOf(schema.Schema, "t").DataType);
            Assert.Equal(TimeUnit.Microsecond, time.Unit);

            Assert.Equal(ArrowTypeId.String, FieldOf(schema.Schema, "geo").DataType.TypeId);
            Assert.Equal(ArrowTypeId.String, FieldOf(schema.Schema, "j").DataType.TypeId);

            var array = Assert.IsType<ListType>(FieldOf(schema.Schema, "arr").DataType);
            Assert.Equal(ArrowTypeId.Int64, array.ValueDataType.TypeId);

            var timestamp = Assert.IsType<TimestampType>(FieldOf(schema.Schema, "ts").DataType);
            Assert.Equal(TimeUnit.Microsecond, timestamp.Unit);
            Assert.True(timestamp.Timezone is "UTC" or "+00:00");
        });
    }

    [SkippableFact]
    public async Task Sink_append_merge_and_replace_round_trip_reading_back_with_a_query_job()
    {
        SkipUnlessLive();
        await RunLiveFactAsync(async ctx =>
        {
            const string table = "roundtrip";

            ISinkConnector sinkConnector = new BqConnector();
            await using var sink = await sinkConnector.OpenAsync(new ConnectorConfig(ConnectionConfig(ctx.Project)), CancellationToken.None);

            var appendSpec = new OutputSpec("bigquery", $"{ctx.Dataset}.{table}", "append", "fail_on_change", new Dictionary<string, object?>());
            await WriteAsync(sink, appendSpec, [IdNameBatch((1, "a"), (2, "b"), (3, "c"))]);
            await WriteAsync(sink, appendSpec, [IdNameBatch((4, "d"), (5, "e"), (6, "f"))]);

            var afterAppends = await ReadBackIdNameAsync(ctx, table);
            Assert.Equal(6, afterAppends.Count);

            var mergeSpec = new OutputSpec("bigquery", $"{ctx.Dataset}.{table}", "merge", "fail_on_change", new Dictionary<string, object?>())
            {
                Keys = ["id"],
            };
            await WriteAsync(sink, mergeSpec, [IdNameBatch((1, "updated"), (7, "g"))]);

            var afterMerge = await ReadBackIdNameAsync(ctx, table);
            Assert.Equal(7, afterMerge.Count);
            Assert.Equal("updated", afterMerge.Single(r => r.Id == 1).Name);
            Assert.Equal("g", afterMerge.Single(r => r.Id == 7).Name);

            var replaceSpec = new OutputSpec("bigquery", $"{ctx.Dataset}.{table}", "replace", "fail_on_change", new Dictionary<string, object?>());
            await WriteAsync(sink, replaceSpec, [IdNameBatch((100, "only"))]);

            var afterReplace = await ReadBackIdNameAsync(ctx, table);
            var only = Assert.Single(afterReplace);
            Assert.Equal(100, only.Id);
            Assert.Equal("only", only.Name);
        });
    }

    [SkippableFact]
    public async Task A_view_is_refused_by_the_Storage_Read_API_with_PZBQ0204()
    {
        SkipUnlessLive();
        await RunLiveFactAsync(async ctx =>
        {
            const string baseTable = "viewbase";
            const string view = "aview";
            await ExecuteDdlAsync(ctx, $"create table `{ctx.Project}.{ctx.Dataset}.{baseTable}` (id INT64)").ConfigureAwait(false);
            await ExecuteDdlAsync(ctx, $"insert into `{ctx.Project}.{ctx.Dataset}.{baseTable}` (id) values (1), (2)").ConfigureAwait(false);
            await ExecuteDdlAsync(ctx,
                $"create view `{ctx.Project}.{ctx.Dataset}.{view}` as select * from `{ctx.Project}.{ctx.Dataset}.{baseTable}`")
                .ConfigureAwait(false);

            ISourceConnector connector = new BqConnector();
            await using var source = await connector.OpenAsync(new ConnectorConfig(ConnectionConfig(ctx.Project)), CancellationToken.None);
            var spec = new DatasetSpec("bigquery", $"{ctx.Dataset}.{view}", new Dictionary<string, object?> { ["streams"] = 1 });

            var ex = await Assert.ThrowsAsync<PzConnectorException>(() => source.GetSchemaAsync(spec, CancellationToken.None).AsTask());

            Assert.Contains("PZBQ0204", ex.Message, StringComparison.Ordinal);
            Assert.Contains("query:", ex.Message, StringComparison.Ordinal);
        });
    }

    [SkippableFact]
    public async Task Requesting_three_streams_reads_every_row_of_a_120_row_table_exactly_once()
    {
        SkipUnlessLive();
        await RunLiveFactAsync(async ctx =>
        {
            const string table = "streamtable";
            await ExecuteDdlAsync(ctx,
                $"create table `{ctx.Project}.{ctx.Dataset}.{table}` as select id from unnest(generate_array(0, 119)) as id")
                .ConfigureAwait(false);

            ISourceConnector connector = new BqConnector();
            await using var source = await connector.OpenAsync(new ConnectorConfig(ConnectionConfig(ctx.Project)), CancellationToken.None);
            var spec = new DatasetSpec("bigquery", $"{ctx.Dataset}.{table}", new Dictionary<string, object?> { ["streams"] = 3 });

            var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);

            var ids = new List<long>();
            foreach (var partition in partitions)
            {
                await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
                {
                    var column = (Int64Array)batch.Column(0);
                    for (var row = 0; row < batch.Length; row++)
                    {
                        ids.Add(column.GetValue(row)!.Value);
                    }
                }
            }

            Assert.Equal(120, ids.Count);
            Assert.Equal(Enumerable.Range(0, 120).Select(i => (long)i), ids.Distinct().Order());

            // Google may grant fewer than the requested 3 streams for a table this small (BigQuery
            // sizes the stream count to the data, not the request) -- the union-of-partitions
            // invariant above is what this fact proves regardless of how many it actually granted.
        });
    }
}
