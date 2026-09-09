using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace Pz.Connector.BigQuery;

/// <summary>The resumable-upload half of <see cref="BqRestClient"/> (split out to keep
/// <c>BqRestClient.cs</c> to the metadata/job-status calls): two legs against two different hosts --
/// <c>POST .../upload/bigquery/v2/.../jobs?uploadType=resumable</c> against <c>RestBase</c> to open a
/// session, then <c>PUT</c> the bytes to whatever absolute <c>Location</c> that leg answers with.
/// That <c>Location</c> is a session URI Google mints and owns; it is used exactly as given, never
/// re-composed against <c>RestBase</c> -- except for the one case no real Google response can ever
/// produce (<see cref="RewriteUnspecifiedHost"/>).</summary>
internal sealed partial class BqRestClient
{
    /// <summary>Uploads <paramref name="content"/> as the load job's data. The caller keeps
    /// ownership of <paramref name="content"/> -- this method never disposes it, so an engine-driven
    /// retry of the same upload can rewind and re-read the same stream. That is why the
    /// <see cref="StreamContent"/> and the <see cref="HttpRequestMessage"/> wrapping it are
    /// deliberately never disposed here: <see cref="HttpContent"/>.Dispose() disposes whatever
    /// stream it wraps, and an <see cref="HttpRequestMessage"/>'s own Dispose cascades into its
    /// Content -- either one disposing would reach the caller's stream. Neither type holds an
    /// unmanaged resource of its own worth releasing once the request has been sent.</summary>
    public async Task<BqJob> UploadLoadJobAsync(string project, BqJob job, Stream content, long length, CancellationToken ct)
    {
        var jobId = job.JobReference?.JobId ?? throw new ArgumentException("job.JobReference.JobId is required", nameof(job));
        var context = $"loading into staging via job {jobId}";
        var initiatePath = $"upload/bigquery/v2/projects/{Uri.EscapeDataString(project)}/jobs?uploadType=resumable";
        var json = JsonSerializer.Serialize(job, BqJsonContext.Default.BqJob);
        var initiateHeaders = new Dictionary<string, string>
        {
            ["X-Upload-Content-Type"] = "application/octet-stream",
            ["X-Upload-Content-Length"] = length.ToString(CultureInfo.InvariantCulture),
        };

        var location = await InitiateResumableUploadAsync(initiatePath, json, initiateHeaders, context, ct).ConfigureAwait(false);

        var request = new HttpRequestMessage(HttpMethod.Put, location);
        var streamContent = new StreamContent(content);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        streamContent.Headers.ContentLength = length;
        request.Content = streamContent;

        using var putResponse = await SendCoreAsync(request, context, ct).ConfigureAwait(false);
        var putBody = await ReadBodyAsync(putResponse, context, ct).ConfigureAwait(false);
        if ((int)putResponse.StatusCode is < 200 or >= 300)
        {
            var (reason, message) = ParseError(putBody, putResponse.ReasonPhrase);
            throw BqErrors.FromRest((int)putResponse.StatusCode, reason, message, RetryAfterOf(putResponse), r, context);
        }

        return DeserializeOrThrow(putBody, BqJsonContext.Default.BqJob, context);
    }

    private async Task<Uri> InitiateResumableUploadAsync(
        string initiatePath, string json, IReadOnlyDictionary<string, string> headers, string context, CancellationToken ct)
    {
        using var response = await SendRawAsync(HttpMethod.Post, initiatePath, json, headers, context, ct).ConfigureAwait(false);
        if ((int)response.StatusCode is < 200 or >= 300)
        {
            var body = await ReadBodyAsync(response, context, ct).ConfigureAwait(false);
            var (reason, message) = ParseError(body, response.ReasonPhrase);
            throw BqErrors.FromRest((int)response.StatusCode, reason, message, RetryAfterOf(response), r, context);
        }

        if (response.Headers.Location is { IsAbsoluteUri: true } location)
        {
            return RewriteUnspecifiedHost(location);
        }

        throw BqErrors.FromRest((int)response.StatusCode, "missingUploadLocation",
            "resumable upload initiation returned no Location header to PUT the bytes to", null, r, context);
    }

    /// <summary>A dockerized emulator binds <c>0.0.0.0</c>/<c>::0</c> and echoes that same
    /// unspecified address straight back in its own <c>Location</c> header instead of a host this
    /// process can actually connect to -- confirmed against
    /// <c>ghcr.io/goccy/bigquery-emulator:0.8.1</c>, whose resumable-upload initiation answers
    /// <c>Location: http://0.0.0.0:9050/...</c> regardless of what was requested. Real BigQuery's
    /// <c>Location</c> is always a fully qualified <c>googleapis.com</c> URI, so this only ever
    /// rewrites against a test double: everything but scheme/host/port is kept exactly as given
    /// (the emulator's own upload-session query string, most importantly) -- the scheme is also
    /// replaced only because the endpoint actually used may not share the unusable URI's own
    /// (this emulator is always plain <c>http</c> either way).</summary>
    private Uri RewriteUnspecifiedHost(Uri location)
    {
        if (!IPAddress.TryParse(location.Host, out var host) || !(host.Equals(IPAddress.Any) || host.Equals(IPAddress.IPv6Any)))
        {
            return location;
        }

        var builder = new UriBuilder(location) { Scheme = cfg.RestBase.Scheme, Host = cfg.RestBase.Host, Port = cfg.RestBase.Port };
        return builder.Uri;
    }
}
