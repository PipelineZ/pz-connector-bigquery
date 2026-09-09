using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Pz.Connector.BigQuery.Tests;

[CollectionDefinition("bigquery")]
public sealed class BqCollection : ICollectionFixture<BqFixture>;

/// <summary>One BigQuery emulator per test run, pre-created with dataset <see cref="Dataset"/>
/// under project <see cref="Project"/>. Table and dataset names are unique per call so facts never
/// share state. Helpers talk to the emulator with a plain HttpClient, deliberately not through the
/// connector under test: a fact that used that code to seed and verify its own assertions would
/// prove nothing.</summary>
public sealed class BqFixture : IAsyncLifetime
{
    public const string Image = "ghcr.io/goccy/bigquery-emulator:0.8.1";
    public const string Project = "test";
    public const string Dataset = "e2e";

    private IContainer? _container;
    private HttpClient? _http;

    public string RestUrl { get; private set; } = "";

    public string StorageEndpoint { get; private set; } = "";

    private HttpClient Http => _http ?? throw new InvalidOperationException("the emulator is not running");

    public async Task InitializeAsync()
    {
        if (!DockerFacts.IsAvailable)
        {
            return;
        }

        // Built here rather than in a field initializer: Build() resolves and pings the docker
        // endpoint, so a constructor that built it would throw before the probe above could no-op --
        // and a collection fixture that throws is a failed fixture, not a skip.
        _container = new ContainerBuilder(Image)
            .WithCommand("--project=" + Project, "--dataset=" + Dataset)
            .WithPortBinding(9050, true)
            .WithPortBinding(9060, true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r.ForPort(9050).ForPath($"/bigquery/v2/projects/{Project}/datasets")))
            .Build();
        await _container.StartAsync().ConfigureAwait(false);
        RestUrl = $"http://{_container.Hostname}:{_container.GetMappedPublicPort(9050)}";
        StorageEndpoint = $"{_container.Hostname}:{_container.GetMappedPublicPort(9060)}";
        _http = new HttpClient { BaseAddress = new Uri(RestUrl), Timeout = TimeSpan.FromMinutes(2) };
    }

    public async Task DisposeAsync()
    {
        _http?.Dispose();
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Dictionary<string, object?> ConnectionConfig(string? stagingDataset = null)
    {
        var config = new Dictionary<string, object?>
        {
            ["project"] = Project,
            ["auth"] = "none",
            ["endpoint"] = RestUrl,
            ["storage_endpoint"] = StorageEndpoint,
        };

        if (stagingDataset is not null)
        {
            config["staging_dataset"] = stagingDataset;
        }

        return config;
    }

    /// <summary>A name unique enough that concurrent facts never collide: table/dataset ids allow
    /// only <c>[A-Za-z0-9_]</c>, so the random suffix is hex, not a GUID's dashed form.</summary>
    public static string NewName(string prefix)
    {
        var bytes = new byte[6];
        Random.Shared.NextBytes(bytes);
        return $"{prefix}_{Convert.ToHexString(bytes).ToLowerInvariant()}";
    }

    /// <summary><paramref name="fieldsJson"/> is a BigQuery field-list JSON array, e.g.
    /// <c>[{"name":"id","type":"INTEGER"}]</c> -- legacy type names only (INTEGER/FLOAT/BOOLEAN/...);
    /// the emulator's Storage Read API rejects INT64 and friends when they appear in a table schema.</summary>
    public async Task CreateTableAsync(string table, string fieldsJson, string? dataset = null)
    {
        dataset ??= Dataset;
        // Built with concatenation, not raw-string interpolation: fieldsJson is itself a JSON
        // fragment, and its own closing braces run into the interpolation delimiter's braces.
        var body = "{\"tableReference\":{\"projectId\":\"" + Project + "\",\"datasetId\":\"" + dataset
            + "\",\"tableId\":\"" + table + "\"},\"schema\":{\"fields\":" + fieldsJson + "}}";
        await SendAsync(HttpMethod.Post, $"/bigquery/v2/projects/{Project}/datasets/{dataset}/tables", body).ConfigureAwait(false);
    }

    // Any non-2xx (a genuine 404 as much as a transient 500) reads as "table absent" -- deliberate
    // for an existence check, where "not found" is the expected non-exceptional outcome.
    public async Task<bool> TableExistsAsync(string table, string? dataset = null)
    {
        dataset ??= Dataset;
        using var response = await Http.GetAsync($"/bigquery/v2/projects/{Project}/datasets/{dataset}/tables/{table}").ConfigureAwait(false);
        return response.IsSuccessStatusCode;
    }

    // Same non-2xx-means-absent shape as TableExistsAsync, for the same reason.
    public async Task<JsonElement?> GetTableAsync(string table)
    {
        using var response = await Http.GetAsync($"/bigquery/v2/projects/{Project}/datasets/{Dataset}/tables/{table}").ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            return null;
        }

        var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        return JsonDocument.Parse(text).RootElement.Clone();
    }

    public Task DeleteTableAsync(string table) =>
        SendAsync(HttpMethod.Delete, $"/bigquery/v2/projects/{Project}/datasets/{Dataset}/tables/{table}", null);

    public Task CreateDatasetAsync(string dataset) =>
        SendAsync(HttpMethod.Post, $"/bigquery/v2/projects/{Project}/datasets",
            "{\"datasetReference\":{\"projectId\":\"" + Project + "\",\"datasetId\":\"" + dataset + "\"}}");

    /// <summary>Loads rows with <c>tabledata.insertAll</c> in chunks of 500 -- the emulator's own
    /// batch ceiling for a single insertAll call.</summary>
    public async Task InsertRowsAsync(string table, IEnumerable<string> rowJson)
    {
        foreach (var chunk in rowJson.Chunk(500))
        {
            var rows = string.Join(",", chunk.Select(r => $$"""{"json":{{r}}}"""));
            var text = await SendAsync(HttpMethod.Post,
                $"/bigquery/v2/projects/{Project}/datasets/{Dataset}/tables/{table}/insertAll",
                $$"""{"rows":[{{rows}}]}""").ConfigureAwait(false);
            using var doc = JsonDocument.Parse(text);
            if (doc.RootElement.TryGetProperty("insertErrors", out var errors))
            {
                throw new InvalidOperationException($"insertAll reported errors: {errors}");
            }
        }
    }

    /// <summary>Loads <paramref name="csv"/> into <paramref name="table"/> with a multipart
    /// <c>jobs.insert</c> load job (source format CSV, autodetect off -- the emulator does not infer
    /// types reliably enough for the large-dataset fixtures that use this path). The destination
    /// table must already exist (via <see cref="CreateTableAsync"/>): its schema is read back and
    /// repeated verbatim as the job's explicit <c>schema</c>, since the emulator does not resolve an
    /// omitted load-job schema against an existing destination table the way real BigQuery does.
    /// The emulator unconditionally discards the CSV's first line as a header, ignoring
    /// <c>skipLeadingRows</c> entirely -- <paramref name="csv"/> must carry one throwaway leading
    /// line before the real data or that first data row is silently lost. Polls until the job
    /// reaches a terminal state.</summary>
    public async Task LoadCsvAsync(string table, string csv)
    {
        var existing = await GetTableAsync(table).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"table '{table}' does not exist; call CreateTableAsync first");
        var fieldsJson = existing.GetProperty("schema").GetProperty("fields").GetRawText();

        var jobConfig = "{\"configuration\":{\"load\":{\"sourceFormat\":\"CSV\",\"autodetect\":false,\"schema\":{\"fields\":"
            + fieldsJson + "},\"destinationTable\":{\"projectId\":\"" + Project + "\",\"datasetId\":\"" + Dataset
            + "\",\"tableId\":\"" + table + "\"}}}}";

        var boundary = $"pz-{Guid.NewGuid():N}";
        using var content = new MultipartContent("related", boundary);
        var jobPart = new StringContent(jobConfig, Encoding.UTF8, "application/json");
        content.Add(jobPart);
        var csvPart = new StringContent(csv, Encoding.UTF8, "text/csv");
        content.Add(csvPart);

        using var request = new HttpRequestMessage(HttpMethod.Post, $"/upload/bigquery/v2/projects/{Project}/jobs?uploadType=multipart")
        {
            Content = content,
        };
        using var response = await Http.SendAsync(request).ConfigureAwait(false);
        var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"load job submit -> {(int)response.StatusCode}: {text}");
        }

        using var submitDoc = JsonDocument.Parse(text);
        var jobId = submitDoc.RootElement.GetProperty("jobReference").GetProperty("jobId").GetString();
        await PollJobAsync(jobId!).ConfigureAwait(false);
    }

    private async Task PollJobAsync(string jobId)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var text = await SendAsync(HttpMethod.Get, $"/bigquery/v2/projects/{Project}/jobs/{jobId}", null).ConfigureAwait(false);
            using var doc = JsonDocument.Parse(text);
            var status = doc.RootElement.GetProperty("status");
            if (status.TryGetProperty("state", out var state) && state.GetString() == "DONE")
            {
                if (status.TryGetProperty("errorResult", out var error))
                {
                    throw new InvalidOperationException($"load job {jobId} failed: {error}");
                }

                return;
            }

            await Task.Delay(100).ConfigureAwait(false);
        }

        throw new TimeoutException($"load job {jobId} did not reach DONE in time");
    }

    /// <summary>Runs <paramref name="sql"/> via <c>jobs.query</c> and maps every result row to a
    /// JSON object keyed by the query's own returned schema (the wire shape is a schema-relative
    /// array, <c>rows[].f[].v</c>, not a self-describing object). <paramref name="maxResults"/>, when
    /// given, is passed straight through in the request body -- against real BigQuery it forces a
    /// small page size; this emulator ignores it and always returns every row in one page (verified:
    /// a 3-row query with <c>maxResults: 1</c> still came back with all 3 rows and no
    /// <c>pageToken</c>), so <see cref="AssembleRows"/> carries the paging-assembly unit coverage
    /// instead of a fixture test. A response with <c>jobComplete: false</c> is polled via
    /// <c>jobs.getQueryResults</c> (bounded, same 100 x 100ms shape as <see cref="PollJobAsync"/>)
    /// until it turns true, and every <c>pageToken</c> the server hands back is followed the same way
    /// until none remains, so a large or slow query never comes back silently truncated.</summary>
    public async Task<List<JsonElement>> QueryAsync(string sql, int? maxResults = null)
    {
        var body = maxResults is int max
            ? JsonSerializer.Serialize(new { query = sql, useLegacySql = false, maxResults = max })
            : JsonSerializer.Serialize(new { query = sql, useLegacySql = false });
        var text = await SendAsync(HttpMethod.Post, $"/bigquery/v2/projects/{Project}/queries", body).ConfigureAwait(false);

        var page = ParsePage(text);
        var jobId = GetJobId(page);
        var location = GetLocation(page);

        for (var attempt = 0; !IsComplete(page); attempt++)
        {
            if (attempt >= 100)
            {
                throw new TimeoutException($"query job {jobId} did not complete in time");
            }

            await Task.Delay(100).ConfigureAwait(false);
            var pollText = await SendAsync(HttpMethod.Get, QueryResultsPath(jobId, location, null), null).ConfigureAwait(false);
            page = ParsePage(pollText);
        }

        var pages = new List<JsonElement> { page };
        var pageToken = GetPageToken(page);
        while (!string.IsNullOrEmpty(pageToken))
        {
            var pageText = await SendAsync(HttpMethod.Get, QueryResultsPath(jobId, location, pageToken), null).ConfigureAwait(false);
            var nextPage = ParsePage(pageText);
            pages.Add(nextPage);
            pageToken = GetPageToken(nextPage);
        }

        return AssembleRows(pages);
    }

    /// <summary>Runs a DDL/DML statement (e.g. <c>CREATE VIEW ...</c>) via <c>jobs.query</c> to
    /// completion, ignoring any result rows. Unlike <see cref="QueryAsync"/>, such a statement's
    /// response carries an empty <c>schema</c> object with no <c>fields</c> array -- <see cref="AssembleRows"/>
    /// is built for genuine row-returning queries and throws on that shape, so this reuses the same
    /// polling as <see cref="QueryAsync"/> but stops short of assembling rows.</summary>
    public async Task ExecuteStatementAsync(string sql)
    {
        var body = JsonSerializer.Serialize(new { query = sql, useLegacySql = false });
        var text = await SendAsync(HttpMethod.Post, $"/bigquery/v2/projects/{Project}/queries", body).ConfigureAwait(false);

        var page = ParsePage(text);
        var jobId = GetJobId(page);
        var location = GetLocation(page);

        for (var attempt = 0; !IsComplete(page); attempt++)
        {
            if (attempt >= 100)
            {
                throw new TimeoutException($"statement job {jobId} did not complete in time");
            }

            await Task.Delay(100).ConfigureAwait(false);
            var pollText = await SendAsync(HttpMethod.Get, QueryResultsPath(jobId, location, null), null).ConfigureAwait(false);
            page = ParsePage(pollText);
        }
    }

    /// <summary>Parses one <c>jobs.query</c>/<c>jobs.getQueryResults</c> response and throws if it
    /// carries any <c>errors[]</c> entry -- the wire shape's own signal for a query-level failure,
    /// separate from an HTTP-level non-2xx that <see cref="SendAsync"/> already turns into an
    /// exception.</summary>
    private static JsonElement ParsePage(string text)
    {
        using var doc = JsonDocument.Parse(text);
        var root = doc.RootElement.Clone();
        if (root.TryGetProperty("errors", out var errors) && errors.GetArrayLength() > 0)
        {
            throw new InvalidOperationException($"query reported errors: {errors}");
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

    private static string QueryResultsPath(string? jobId, string? location, string? pageToken)
    {
        var path = $"/bigquery/v2/projects/{Project}/queries/{jobId}";
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

    /// <summary>Combines every already-fetched query-result page into the final row list, using the
    /// schema carried on the first page that has one (later pages of a paged result do not always
    /// repeat it) -- pure and static so it can be exercised by a unit test with synthetic pages,
    /// with no live emulator needed to prove the paging assembly itself is correct.</summary>
    internal static List<JsonElement> AssembleRows(IReadOnlyList<JsonElement> pages)
    {
        List<string>? fields = null;
        var result = new List<JsonElement>();
        foreach (var page in pages)
        {
            if (fields is null && page.TryGetProperty("schema", out var schema))
            {
                fields = schema.GetProperty("fields").EnumerateArray().Select(f => f.GetProperty("name").GetString()!).ToList();
            }

            if (!page.TryGetProperty("rows", out var rows))
            {
                continue;
            }

            foreach (var row in rows.EnumerateArray())
            {
                result.Add(MapRow(row, fields ?? []));
            }
        }

        return result;
    }

    // A RECORD or REPEATED cell's "v" is nested JSON, not a string/number/bool; GetString() then
    // returns null and the GetRawText() fallback below writes that nested JSON back out as an
    // escaped string rather than unwrapping it -- no fixture in this repo yet exercises such a
    // column.
    private static JsonElement MapRow(JsonElement row, List<string> fields)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            var values = row.GetProperty("f").EnumerateArray().ToList();
            for (var i = 0; i < fields.Count && i < values.Count; i++)
            {
                var v = values[i].GetProperty("v");
                writer.WritePropertyName(fields[i]);
                if (v.ValueKind == JsonValueKind.Null)
                {
                    writer.WriteNullValue();
                }
                else
                {
                    writer.WriteStringValue(v.GetString() ?? v.GetRawText());
                }
            }

            writer.WriteEndObject();
        }

        return JsonDocument.Parse(buffer.ToArray()).RootElement;
    }

    private async Task<string> SendAsync(HttpMethod method, string path, string? body)
    {
        using var request = new HttpRequestMessage(method, path);
        if (body is not null)
        {
            request.Content = new StringContent(body, Encoding.UTF8, "application/json");
        }

        using var response = await Http.SendAsync(request).ConfigureAwait(false);
        var text = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"{method} {path} -> {(int)response.StatusCode}: {text}");
        }

        return text;
    }
}
