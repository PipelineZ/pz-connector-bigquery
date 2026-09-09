using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Google.Apis.Auth.OAuth2;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>The BigQuery REST v2 surface this connector touches: table metadata, job
/// insert/get/upload, and a dataset count for connection checks. Every request path is relative to
/// <see cref="BqConnectionConfig.RestBase"/> (never a leading <c>/</c> -- the RFC 3986 lesson: a
/// leading-slash path clobbers a path-bearing base URL's own path on <see cref="Uri"/> composition
/// instead of extending it). The resumable-upload handshake lives in <c>BqUpload.cs</c>, a second
/// partial of this same type, to keep this file to the metadata/job-status calls.</summary>
internal sealed partial class BqRestClient
{
    private const string JsonContentType = "application/json; charset=UTF-8";

    private readonly HttpClient http;
    private readonly BqConnectionConfig cfg;
    private readonly BqRedactor r;
    private readonly ILogger logger;
    private readonly Func<CancellationToken, Task<string?>> tokenSource;

    public BqRestClient(HttpClient http, BqConnectionConfig cfg, GoogleCredential? cred, BqRedactor r, ILogger logger)
        : this(http, cfg, r, logger, ct => BqAuth.AccessTokenAsync(cred, ct))
    {
    }

    /// <summary>The production path always goes through the public constructor above, bound to a
    /// <see cref="GoogleCredential"/>. This one exists so a test can supply a token source that
    /// fails in ways a real <see cref="GoogleCredential"/> is impractical to force (a
    /// <c>TokenResponseException</c> from a bad refresh, say) -- see the token-acquisition-failure
    /// case in <c>BqRestClientTests</c>.</summary>
    internal BqRestClient(
        HttpClient http, BqConnectionConfig cfg, BqRedactor r, ILogger logger, Func<CancellationToken, Task<string?>> tokenSource)
    {
        this.http = http;
        this.cfg = cfg;
        this.r = r;
        this.logger = logger;
        this.tokenSource = tokenSource;
    }

    /// <summary>Exposed so <see cref="BqJob.SubmitAndWaitAsync"/> can classify a job's
    /// <c>errorResult</c> through the same redactor every REST failure on this client already
    /// passes through -- <see cref="BqErrors.FromJobError"/> needs one and takes no client of its
    /// own to get it from.</summary>
    internal BqRedactor Redactor => r;

    public async Task<BqTable?> GetTableAsync(TableRef table, CancellationToken ct)
    {
        var context = $"getting table {table.Dataset}.{table.Table}";
        var (status, body) = await ExecuteAsync(HttpMethod.Get, TablePath(table), null, null, context, ct, HttpStatusCode.NotFound)
            .ConfigureAwait(false);
        return status == HttpStatusCode.NotFound ? null : DeserializeOrThrow(body, BqJsonContext.Default.BqTable, context);
    }

    public async Task InsertTableAsync(BqTable table, CancellationToken ct)
    {
        var reference = table.TableReference ?? throw new ArgumentException("table.TableReference is required", nameof(table));
        var project = reference.ProjectId ?? throw new ArgumentException("table.TableReference.ProjectId is required", nameof(table));
        var dataset = reference.DatasetId ?? throw new ArgumentException("table.TableReference.DatasetId is required", nameof(table));
        var path = $"bigquery/v2/projects/{Uri.EscapeDataString(project)}/datasets/{Uri.EscapeDataString(dataset)}/tables";
        var json = JsonSerializer.Serialize(table, BqJsonContext.Default.BqTable);
        var context = $"creating table {dataset}.{reference.TableId}";
        await ExecuteAsync(HttpMethod.Post, path, json, null, context, ct).ConfigureAwait(false);
    }

    public async Task PatchTableAsync(TableRef table, BqTablePatch patch, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(patch, BqJsonContext.Default.BqTablePatch);
        var context = $"patching table {table.Dataset}.{table.Table}";
        await ExecuteAsync(HttpMethod.Patch, TablePath(table), json, null, context, ct).ConfigureAwait(false);
    }

    public async Task DeleteTableAsync(TableRef table, CancellationToken ct)
    {
        var context = $"deleting table {table.Dataset}.{table.Table}";
        await ExecuteAsync(HttpMethod.Delete, TablePath(table), null, null, context, ct, HttpStatusCode.NotFound).ConfigureAwait(false);
    }

    public async Task<BqJob> InsertJobAsync(string project, BqJob job, CancellationToken ct)
    {
        var jobId = job.JobReference?.JobId ?? throw new ArgumentException("job.JobReference.JobId is required", nameof(job));
        var path = $"bigquery/v2/projects/{Uri.EscapeDataString(project)}/jobs";
        var json = JsonSerializer.Serialize(job, BqJsonContext.Default.BqJob);
        var context = $"submitting job {jobId}";

        var (status, body) = await ExecuteAsync(HttpMethod.Post, path, json, null, context, ct, HttpStatusCode.Conflict).ConfigureAwait(false);
        if (status != HttpStatusCode.Conflict)
        {
            return DeserializeOrThrow(body, BqJsonContext.Default.BqJob, context);
        }

        var (reason, message) = ParseError(body, null);
        if (string.Equals(reason, "duplicate", StringComparison.Ordinal))
        {
            // A retried insert after a lost response hits this -- the job already exists under our
            // own client-generated id, so the right move is to observe it, not submit it again.
            return await GetJobAsync(project, jobId, job.JobReference.Location, ct).ConfigureAwait(false);
        }

        throw BqErrors.FromRest((int)status, reason, message, null, r, context);
    }

    public async Task<BqJob> GetJobAsync(string project, string jobId, string? location, CancellationToken ct)
    {
        var path = $"bigquery/v2/projects/{Uri.EscapeDataString(project)}/jobs/{Uri.EscapeDataString(jobId)}";
        if (!string.IsNullOrEmpty(location))
        {
            path += $"?location={Uri.EscapeDataString(location)}";
        }

        var context = $"getting job {jobId}";
        var (_, body) = await ExecuteAsync(HttpMethod.Get, path, null, null, context, ct).ConfigureAwait(false);
        return DeserializeOrThrow(body, BqJsonContext.Default.BqJob, context);
    }

    public async Task<int> CountDatasetsAsync(string project, CancellationToken ct)
    {
        var path = $"bigquery/v2/projects/{Uri.EscapeDataString(project)}/datasets?maxResults=1000";
        const string context = "listing datasets";
        var (_, body) = await ExecuteAsync(HttpMethod.Get, path, null, null, context, ct).ConfigureAwait(false);
        var list = DeserializeOrThrow(body, BqJsonContext.Default.BqDatasetList, context);
        return list.Datasets?.Length ?? 0;
    }

    private static string TablePath(TableRef table) =>
        $"bigquery/v2/projects/{Uri.EscapeDataString(table.Project)}/datasets/{Uri.EscapeDataString(table.Dataset)}/tables/{Uri.EscapeDataString(table.Table)}";

    /// <summary>Sends one request and returns its status and body, throwing the classified
    /// <see cref="PzConnectorException"/> for any status outside 2xx and outside
    /// <paramref name="tolerate"/> -- the handful of callers that treat a particular status as data
    /// (404 as "does not exist", 409 as "already submitted") pass it and inspect
    /// <paramref name="tolerate"/>'d results themselves; everyone else just gets the throw.</summary>
    private async Task<(HttpStatusCode Status, string Body)> ExecuteAsync(
        HttpMethod method, string relativePath, string? jsonBody, IReadOnlyDictionary<string, string>? extraHeaders,
        string context, CancellationToken ct, params HttpStatusCode[] tolerate)
    {
        using var response = await SendRawAsync(method, relativePath, jsonBody, extraHeaders, context, ct).ConfigureAwait(false);
        var body = await ReadBodyAsync(response, context, ct).ConfigureAwait(false);

        if ((int)response.StatusCode is >= 200 and < 300 || Array.IndexOf(tolerate, response.StatusCode) >= 0)
        {
            return (response.StatusCode, body);
        }

        var (reason, message) = ParseError(body, response.ReasonPhrase);
        throw BqErrors.FromRest((int)response.StatusCode, reason, message, RetryAfterOf(response), r, context);
    }

    /// <summary>Builds and sends one request without inspecting the response -- the resumable
    /// upload handshake in <c>BqUpload.cs</c> needs the live <see cref="HttpResponseMessage"/> itself
    /// (to read its <c>Location</c> header) before deciding whether to throw, so it calls this
    /// directly instead of going through <see cref="ExecuteAsync"/>.</summary>
    private async Task<HttpResponseMessage> SendRawAsync(
        HttpMethod method, string relativePath, string? jsonBody, IReadOnlyDictionary<string, string>? extraHeaders,
        string context, CancellationToken ct)
    {
        var uri = new Uri(cfg.RestBase, relativePath);
        using var request = new HttpRequestMessage(method, uri);
        if (jsonBody is not null)
        {
            request.Content = new StringContent(jsonBody, Encoding.UTF8);
            request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(JsonContentType);
        }

        if (extraHeaders is not null)
        {
            foreach (var (key, value) in extraHeaders)
            {
                request.Headers.TryAddWithoutValidation(key, value);
            }
        }

        return await SendCoreAsync(request, context, ct).ConfigureAwait(false);
    }

    /// <summary>Attaches the bearer token (if any) and sends, through the same transport-exception
    /// classification <see cref="ReadBodyAsync"/> uses -- a dropped connection looks the same
    /// whether it happens while writing the request or while reading the response.</summary>
    private Task<HttpResponseMessage> SendCoreAsync(HttpRequestMessage request, string context, CancellationToken ct) =>
        GuardTransportAsync(async () =>
        {
            var token = await AcquireTokenAsync(context, ct).ConfigureAwait(false);
            if (token is not null)
            {
                r.AddSecret(token);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            }

            // GetLeftPart(UriPartial.Path) drops the query string -- this path is shared by the
            // resumable-upload PUT (BqUpload.cs), whose RequestUri is a session URI Google mints
            // carrying the upload id as a query parameter, a bearer-equivalent capability for that
            // upload that must never reach a log line.
            logger.LogDebug("bigquery {Method} {Uri}", request.Method, request.RequestUri?.GetLeftPart(UriPartial.Path));
            return await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        }, context, ct);

    /// <summary>Reads a response body through the same transport-exception classification
    /// <see cref="SendCoreAsync"/> uses. <c>HttpCompletionOption.ResponseHeadersRead</c> means the
    /// body is not actually pulled off the wire until this call, so a connection dropped mid-body
    /// throws here, not there -- without this, that failure would escape unclassified past every
    /// caller straight out of <c>ExecuteAsync</c>/the resumable-upload handshake.</summary>
    private Task<string> ReadBodyAsync(HttpResponseMessage response, string context, CancellationToken ct) =>
        GuardTransportAsync(() => response.Content.ReadAsStringAsync(ct), context, ct);

    /// <summary>The one place that knows how to turn a transport-level failure into a classified
    /// <see cref="PzConnectorException"/> (or let it alone). A caller-driven cancellation always
    /// propagates unwrapped; an HttpClient-internal timeout arrives as a
    /// <see cref="TaskCanceledException"/> whose <see cref="Exception.InnerException"/> is a
    /// <see cref="TimeoutException"/> (the documented way .NET distinguishes the two since
    /// HttpClient started honoring its own <c>Timeout</c> this way) -- that inner exception, never
    /// the <see cref="OperationCanceledException"/> wrapping it, is what reaches
    /// <see cref="BqErrors.Wrap"/>, which refuses any <see cref="OperationCanceledException"/>
    /// outright.</summary>
    private async Task<T> GuardTransportAsync<T>(Func<Task<T>> action, string context, CancellationToken ct)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException timeout)
        {
            throw BqErrors.Wrap(timeout, r, context);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException or SocketException)
        {
            throw BqErrors.Wrap(ex, r, context);
        }
    }

    /// <summary>Acquiring a token is a distinct failure mode from sending the request that carries
    /// it: a stale/misconfigured credential (an expired refresh token, a revoked service account, a
    /// broken metadata-server lookup) never reaches the network at all, so it must not fall through
    /// <see cref="GuardTransportAsync{T}"/>'s HTTP-shaped classification -- it is always an
    /// authentication problem, never transient, and the underlying exception (a
    /// <c>TokenResponseException</c>, an <see cref="InvalidOperationException"/> from the credential
    /// layer, or anything else the credential implementation throws) may echo request context that
    /// needs the same redaction every other failure message gets.</summary>
    private async Task<string?> AcquireTokenAsync(string context, CancellationToken ct)
    {
        try
        {
            return await tokenSource(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new PzConnectorException(
                BqCodes.Message(BqCodes.Remote_Unauthenticated, r,
                    $"{context}: could not obtain a bearer token: {ex.Message} -- check the service-account key or Application Default Credentials"),
                isTransient: false, innerException: ex);
        }
    }

    private static (string? Reason, string? Message) ParseError(string body, string? statusLineFallback)
    {
        if (!string.IsNullOrWhiteSpace(body))
        {
            BqErrorEnvelope? envelope = null;
            try
            {
                envelope = JsonSerializer.Deserialize(body, BqJsonContext.Default.BqErrorEnvelope);
            }
            catch (JsonException)
            {
                // Not a BigQuery error envelope (an emulator's plain-text 500, say) -- fall through
                // to the status-line fallback below.
            }

            if (envelope?.Error is { } error)
            {
                var reason = error.Errors is { Length: > 0 } errors ? errors[0].Reason : null;
                return (reason, error.Message);
            }
        }

        return (null, statusLineFallback);
    }

    private static TimeSpan? RetryAfterOf(HttpResponseMessage response)
    {
        var retryAfter = response.Headers.RetryAfter;
        if (retryAfter is null)
        {
            return null;
        }

        if (retryAfter.Delta is { } delta)
        {
            return delta;
        }

        if (retryAfter.Date is { } date)
        {
            var remaining = date - DateTimeOffset.UtcNow;
            return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
        }

        return null;
    }

    private T DeserializeOrThrow<T>(string body, JsonTypeInfo<T> typeInfo, string context)
        where T : class
    {
        T? result;
        try
        {
            result = JsonSerializer.Deserialize(body, typeInfo);
        }
        catch (JsonException)
        {
            // A 2xx status only means the transport succeeded -- a rewriting proxy or a
            // misbehaving emulator can still answer with a body that is not the JSON this call
            // expects (or not JSON at all). That must surface as a classified PZBQ#### error like
            // every other failure on this client, never a raw JsonException.
            throw new PzConnectorException(BqCodes.Message(BqCodes.Remote_Other, r, $"{context}: response body did not parse"), isTransient: false);
        }

        return result ?? throw new PzConnectorException(BqCodes.Message(BqCodes.Remote_Other, r, $"{context}: response body did not parse"), isTransient: false);
    }
}
