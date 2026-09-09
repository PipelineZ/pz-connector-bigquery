using System.Net;
using System.Text;
using Google.Apis.Auth.OAuth2;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqRestClientTests
{
    private const string TableJson = """{"tableReference":{"projectId":"p","datasetId":"d","tableId":"t"},"type":"TABLE"}""";

    private static BqConnectionConfig Config(string endpoint = "http://fake/")
    {
        var errors = new List<string>();
        var config = BqConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["project"] = "p",
            ["auth"] = "none",
            ["endpoint"] = endpoint,
        }), errors);

        Assert.Empty(errors);
        Assert.NotNull(config);
        return config!;
    }

    private static BqRestClient Client(FakeHandler handler, string endpoint = "http://fake/", GoogleCredential? cred = null, BqRedactor? redactor = null) =>
        new(new HttpClient(handler), Config(endpoint), cred, redactor ?? BqRedactor.None, NullLogger.Instance);

    // The RFC 3986 lesson: a path-bearing base URL ("http://fake/api/v2/") composed with a *relative*
    // request path must keep the base's own path segment, not have it clobbered the way a
    // leading-slash request path would clobber it.
    [Fact]
    public async Task GetTableAsync_composes_relative_to_a_path_bearing_endpoint()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/api/v2/bigquery/v2/projects/p/datasets/d/tables/t", 200, TableJson);
        var client = Client(handler, "http://fake/api/v2/");

        var table = await client.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None);

        Assert.NotNull(table);
        Assert.Equal("p", table!.TableReference?.ProjectId);
        Assert.Single(handler.Requests);
        Assert.Equal("http://fake/api/v2/bigquery/v2/projects/p/datasets/d/tables/t", handler.Requests[0].Url.ToString());
    }

    [Fact]
    public async Task GetTableAsync_returns_null_on_404()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/d/tables/missing", 404,
            """{"error":{"code":404,"message":"not found","errors":[{"reason":"notFound"}]}}""");
        var client = Client(handler);

        var table = await client.GetTableAsync(new TableRef("p", "d", "missing"), CancellationToken.None);

        Assert.Null(table);
    }

    [Fact]
    public async Task GetTableAsync_malformed_2xx_body_classifies_as_PZBQ0409_not_a_raw_JsonException()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/d/tables/t", 200, "not json at all {{{");
        var client = Client(handler);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => client.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0409", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetTableAsync_403_access_denied_classifies_as_PZBQ0405()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/d/tables/t", 403,
            """{"error":{"code":403,"message":"forbidden","errors":[{"reason":"accessDenied"}]}}""");
        var client = Client(handler);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => client.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0405", ex.Message);
    }

    [Fact]
    public async Task Authorization_header_is_present_with_a_credential_and_absent_with_none()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/d/tables/t", 200, TableJson);

        var withCred = Client(handler, cred: GoogleCredential.FromAccessToken("tok"), redactor: new BqRedactor([]));
        await withCred.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None);
        Assert.Equal("Bearer tok", handler.Requests[0].Headers["Authorization"]);

        handler.Requests.Clear();
        var withoutCred = Client(handler, cred: null);
        await withoutCred.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None);
        Assert.False(handler.Requests[0].Headers.ContainsKey("Authorization"));
    }

    [Fact]
    public async Task A_token_seen_on_a_request_is_masked_when_the_server_echoes_it_back_in_an_error()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/d/tables/t", 403,
            """{"error":{"code":403,"message":"credential tok rejected","errors":[{"reason":"accessDenied"}]}}""");
        var redactor = new BqRedactor([]);
        var client = Client(handler, cred: GoogleCredential.FromAccessToken("tok"), redactor: redactor);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => client.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None));

        Assert.DoesNotContain("tok rejected", ex.Message);
        Assert.Contains(BqRedactor.Mask, ex.Message);
    }

    [Fact]
    public async Task InsertTableAsync_posts_to_the_dataset_tables_collection()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/datasets/d/tables", 200, TableJson);
        var client = Client(handler);
        var table = new BqTable(new BqTableReference("p", "d", "t"), new BqTableSchema([new BqFieldSchema("c", "STRING", null, null, null, null)]), "TABLE", null);

        await client.InsertTableAsync(table, CancellationToken.None);

        Assert.Single(handler.Requests);
        Assert.Contains("\"tableId\":\"t\"", handler.Requests[0].Body);
    }

    [Fact]
    public async Task PatchTableAsync_sends_HTTP_PATCH_with_the_expiration_body()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Patch, "/bigquery/v2/projects/p/datasets/d/tables/t", 200, TableJson);
        var client = Client(handler);

        await client.PatchTableAsync(new TableRef("p", "d", "t"), new BqTablePatch("12345"), CancellationToken.None);

        Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Patch, handler.Requests[0].Method);
        Assert.Equal("""{"expirationTime":"12345"}""", handler.Requests[0].Body);
    }

    [Fact]
    public async Task DeleteTableAsync_tolerates_404()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Delete, "/bigquery/v2/projects/p/datasets/d/tables/gone", 404,
            """{"error":{"code":404,"message":"not found","errors":[{"reason":"notFound"}]}}""");
        var client = Client(handler);

        await client.DeleteTableAsync(new TableRef("p", "d", "gone"), CancellationToken.None);
    }

    [Fact]
    public async Task CountDatasetsAsync_counts_the_returned_page()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets?maxResults=1000", 200,
            """{"datasets":[{"id":"p:a"},{"id":"p:b"},{"id":"p:c"}]}""");
        var client = Client(handler);

        var count = await client.CountDatasetsAsync("p", CancellationToken.None);

        Assert.Equal(3, count);
    }

    [Fact]
    public async Task InsertJobAsync_409_duplicate_falls_through_to_GetJobAsync()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/jobs", 409,
            """{"error":{"code":409,"message":"already exists","errors":[{"reason":"duplicate"}]}}""");
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/jobs/job1?location=US", 200, JobJson("job1", "DONE", "US"));
        var client = Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", "US"), null, null);

        var result = await client.InsertJobAsync("p", job, CancellationToken.None);

        Assert.Equal("DONE", result.Status?.State);
        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal(HttpMethod.Post, handler.Requests[0].Method);
        Assert.Equal(HttpMethod.Get, handler.Requests[1].Method);
    }

    [Fact]
    public async Task InsertJobAsync_409_for_a_reason_other_than_duplicate_still_throws()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/bigquery/v2/projects/p/jobs", 409,
            """{"error":{"code":409,"message":"already running","errors":[{"reason":"jobAlreadyRunning"}]}}""");
        var client = Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);

        await Assert.ThrowsAsync<PzConnectorException>(() => client.InsertJobAsync("p", job, CancellationToken.None));
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task UploadLoadJobAsync_performs_the_resumable_two_step_handshake()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", 200, "ignored",
            new Dictionary<string, string> { ["Location"] = "http://fake-upload/session/abc" });
        handler.Add(HttpMethod.Put, "/session/abc", 200, JobJson("job1", "DONE"));
        var client = Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);
        var bytes = Encoding.UTF8.GetBytes("row1\nrow2\n");
        using var content = new MemoryStream(bytes);

        var result = await client.UploadLoadJobAsync("p", job, content, bytes.Length, CancellationToken.None);

        Assert.Equal("DONE", result.Status?.State);
        Assert.Equal(2, handler.Requests.Count);

        var initiate = handler.Requests[0];
        Assert.Equal(HttpMethod.Post, initiate.Method);
        Assert.Contains("\"jobId\":\"job1\"", initiate.Body);
        Assert.Equal("application/octet-stream", initiate.Headers["X-Upload-Content-Type"]);
        Assert.Equal(bytes.Length.ToString(), initiate.Headers["X-Upload-Content-Length"]);

        var put = handler.Requests[1];
        Assert.Equal(HttpMethod.Put, put.Method);
        Assert.Equal("http://fake-upload/session/abc", put.Url.ToString());
        Assert.Equal("row1\nrow2\n", put.Body);
        Assert.Equal("application/octet-stream", put.Headers["Content-Type"]);
    }

    [Fact]
    public async Task UploadLoadJobAsync_does_not_dispose_the_callers_stream()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", 200, "ignored",
            new Dictionary<string, string> { ["Location"] = "http://fake-upload/session/abc" });
        handler.Add(HttpMethod.Put, "/session/abc", 200, JobJson("job1", "DONE"));
        var client = Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);
        var bytes = Encoding.UTF8.GetBytes("row1\nrow2\n");
        var content = new RecordingStream(new MemoryStream(bytes));

        await client.UploadLoadJobAsync("p", job, content, bytes.Length, CancellationToken.None);

        // An engine-driven retry re-reads the same stream from the start -- proving that requires
        // more than "Dispose was never called": the stream must still actually be usable afterward.
        Assert.False(content.WasDisposed);
        content.Position = 0;
        using var reader = new StreamReader(content);
        Assert.Equal("row1\nrow2\n", await reader.ReadToEndAsync());
    }

    // Confirmed against the live emulator (ghcr.io/goccy/bigquery-emulator:0.8.1): its resumable-
    // upload initiation answers "Location: http://0.0.0.0:9050/..." regardless of what host was
    // actually requested -- it binds 0.0.0.0 and echoes that same unspecified address back, which is
    // not a host this process can connect to. Real BigQuery's Location is always a fully qualified
    // googleapis.com URI, so BqRestClient rewrites only this one unusable shape, keeping the rest of
    // the URI (the emulator's own upload-session query string) untouched.
    [Theory]
    [InlineData("http://0.0.0.0:9050/upload/session/abc?upload_id=x")]
    [InlineData("http://[::0]:9050/upload/session/abc?upload_id=x")]
    public async Task UploadLoadJobAsync_rewrites_an_unspecified_host_in_Location_to_the_endpoint_actually_used(string location)
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", 200, "ignored",
            new Dictionary<string, string> { ["Location"] = location });
        handler.Add(HttpMethod.Put, "/upload/session/abc?upload_id=x", 200, JobJson("job1", "DONE"));
        var client = Client(handler, "http://fake:1234/");
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);
        using var content = new MemoryStream(Encoding.UTF8.GetBytes("row1\n"));

        var result = await client.UploadLoadJobAsync("p", job, content, 5, CancellationToken.None);

        Assert.Equal("DONE", result.Status?.State);
        var put = handler.Requests[1];
        Assert.Equal("http://fake:1234/upload/session/abc?upload_id=x", put.Url.ToString());
    }

    [Fact]
    public async Task UploadLoadJobAsync_leaves_a_real_upload_hosts_Location_untouched()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", 200, "ignored",
            new Dictionary<string, string> { ["Location"] = "http://fake-upload/session/abc" });
        handler.Add(HttpMethod.Put, "/session/abc", 200, JobJson("job1", "DONE"));
        var client = Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);
        using var content = new MemoryStream(Encoding.UTF8.GetBytes("row1\n"));

        await client.UploadLoadJobAsync("p", job, content, 5, CancellationToken.None);

        Assert.Equal("http://fake-upload/session/abc", handler.Requests[1].Url.ToString());
    }

    [Fact]
    public async Task UploadLoadJobAsync_missing_Location_header_is_PZBQ0409_non_transient()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Post, "/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", 200, "ignored");
        var client = Client(handler);
        var job = new BqJob(new BqJobReference("p", "job1", null), null, null);
        using var content = new MemoryStream([1, 2, 3]);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => client.UploadLoadJobAsync("p", job, content, 3, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0409", ex.Message);
    }

    [Fact]
    public async Task A_transport_exception_wraps_as_transient_PZBQ0401()
    {
        var client = new BqRestClient(new HttpClient(new ThrowingHandler(new HttpRequestException("connection refused"))),
            Config(), null, BqRedactor.None, NullLogger.Instance);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => client.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None));

        Assert.True(ex.IsTransient);
        Assert.Contains("PZBQ0401", ex.Message);
    }

    [Fact]
    public async Task An_HttpClient_internal_timeout_wraps_as_transient_rather_than_propagating_as_cancellation()
    {
        // HttpClient.Timeout expiring produces a TaskCanceledException whose InnerException is a
        // TimeoutException -- the documented way .NET tells that apart from the caller's own token
        // firing. BqErrors.Wrap refuses any OperationCanceledException outright, so this path must
        // unwrap to the inner TimeoutException before handing it to Wrap.
        var timeout = new TimeoutException("the operation timed out");
        var client = new BqRestClient(new HttpClient(new ThrowingHandler(new TaskCanceledException("timed out", timeout))),
            Config(), null, BqRedactor.None, NullLogger.Instance);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => client.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None));

        Assert.True(ex.IsTransient);
        Assert.Contains("PZBQ0401", ex.Message);
        Assert.Same(timeout, ex.InnerException);
    }

    [Fact]
    public async Task Caller_cancellation_propagates_unwrapped_rather_than_becoming_a_PzConnectorException()
    {
        using var cts = new CancellationTokenSource();
        var client = new BqRestClient(new HttpClient(new ThrowingHandler(new OperationCanceledException(cts.Token))),
            Config(), null, BqRedactor.None, NullLogger.Instance);
        await cts.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => client.GetTableAsync(new TableRef("p", "d", "t"), cts.Token));
    }

    [Fact]
    public async Task A_transport_exception_while_reading_the_response_body_wraps_as_transient_PZBQ0401()
    {
        // HttpCompletionOption.ResponseHeadersRead means the body is not actually pulled off the
        // wire until BqRestClient reads it -- a connection dropped mid-body must classify the same
        // way a connection refused up front does, not escape as a raw IOException.
        var client = new BqRestClient(new HttpClient(new ThrowingBodyHandler()), Config(), null, BqRedactor.None, NullLogger.Instance);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => client.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None));

        Assert.True(ex.IsTransient);
        Assert.Contains("PZBQ0401", ex.Message);
    }

    [Fact]
    public async Task Token_acquisition_failure_classifies_as_PZBQ0404_non_transient()
    {
        var thrown = new InvalidOperationException("secret-echo");
        var client = new BqRestClient(new HttpClient(new FakeHandler()), Config(), BqRedactor.None, NullLogger.Instance,
            _ => throw thrown);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => client.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0404", ex.Message);
        Assert.Same(thrown, ex.InnerException);
    }

    [Fact]
    public async Task Retry_After_delta_seconds_is_carried_onto_the_exception()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/datasets/d/tables/t", 429,
            """{"error":{"code":429,"message":"slow down","errors":[{"reason":"rateLimitExceeded"}]}}""",
            new Dictionary<string, string> { ["Retry-After"] = "5" });
        var client = Client(handler);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => client.GetTableAsync(new TableRef("p", "d", "t"), CancellationToken.None));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(5), ex.RetryAfter);
    }

    [Fact]
    public async Task GetJobAsync_with_a_null_location_omits_the_location_query_parameter()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/p/jobs/job1", 200, JobJson("job1", "DONE"));
        var client = Client(handler);

        var result = await client.GetJobAsync("p", "job1", null, CancellationToken.None);

        Assert.Equal("DONE", result.Status?.State);
        Assert.Equal("/bigquery/v2/projects/p/jobs/job1", handler.Requests[0].Url.PathAndQuery);
    }

    /// <summary>Throws <paramref name="exception"/> instead of producing a response -- exercises
    /// <c>BqRestClient</c>'s transport-failure classification, which <see cref="FakeHandler"/> (a
    /// route table over real responses) has no way to reach.</summary>
    private sealed class ThrowingHandler(Exception exception) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) =>
            throw exception;
    }

    /// <summary>Answers with headers but a body whose read always fails -- the only way to exercise
    /// a transport failure that happens strictly after <c>HttpClient.SendAsync</c> itself returns.</summary>
    private sealed class ThrowingBodyHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new ThrowingContent() });
    }

    private sealed class ThrowingContent : HttpContent
    {
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
            throw new IOException("connection reset while reading the body");

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }

    /// <summary>Wraps a stream and records whether <c>Dispose</c> was called on it, without ever
    /// disposing the wrapped stream itself -- lets a test prove both that the method under test
    /// never disposed it AND that the stream is still genuinely usable afterward.</summary>
    private sealed class RecordingStream(Stream inner) : Stream
    {
        public bool WasDisposed { get; private set; }

        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => inner.CanSeek;

        public override bool CanWrite => inner.CanWrite;

        public override long Length => inner.Length;

        public override long Position
        {
            get => inner.Position;
            set => inner.Position = value;
        }

        public override void Flush() => inner.Flush();

        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);

        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

        public override void SetLength(long value) => inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);

        protected override void Dispose(bool disposing)
        {
            WasDisposed = true;
        }
    }

    private static string JobJson(string jobId, string state, string? location = null, string? errorReason = null, string? errorMessage = null)
    {
        var sb = new StringBuilder();
        sb.Append("{\"jobReference\":{\"projectId\":\"p\",\"jobId\":\"").Append(jobId).Append('"');
        if (location is not null)
        {
            sb.Append(",\"location\":\"").Append(location).Append('"');
        }

        sb.Append("},\"status\":{\"state\":\"").Append(state).Append('"');
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
}
