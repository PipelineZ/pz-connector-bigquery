using System.Globalization;
using System.Net.Http.Headers;
using System.Text.Json;

namespace Pz.Connector.BigQuery;

/// <summary>The resumable-upload half of <see cref="BqRestClient"/> (split out to keep
/// <c>BqRestClient.cs</c> to the metadata/job-status calls): two legs against two different hosts --
/// <c>POST .../upload/bigquery/v2/.../jobs?uploadType=resumable</c> against <c>RestBase</c> to open a
/// session, then <c>PUT</c> the bytes to whatever absolute <c>Location</c> that leg answers with.
/// That <c>Location</c> is a session URI Google mints and owns; it is used exactly as given, never
/// re-composed against <c>RestBase</c>.</summary>
internal sealed partial class BqRestClient
{
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

        using var request = new HttpRequestMessage(HttpMethod.Put, location);
        using var streamContent = new StreamContent(content);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        streamContent.Headers.ContentLength = length;
        request.Content = streamContent;

        using var putResponse = await SendCoreAsync(request, context, ct).ConfigureAwait(false);
        var putBody = await putResponse.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
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
            var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            var (reason, message) = ParseError(body, response.ReasonPhrase);
            throw BqErrors.FromRest((int)response.StatusCode, reason, message, RetryAfterOf(response), r, context);
        }

        if (response.Headers.Location is { IsAbsoluteUri: true } location)
        {
            return location;
        }

        throw BqErrors.FromRest((int)response.StatusCode, "missingUploadLocation",
            "resumable upload initiation returned no Location header to PUT the bytes to", null, r, context);
    }
}
