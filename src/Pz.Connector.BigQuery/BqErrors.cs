using Grpc.Core;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>Turns a BigQuery failure -- REST error envelope, job <c>errorResult</c>, gRPC status from
/// the Storage Read API, or a transport exception that never got an answer -- into the engine's
/// exception, classified for retry. The reason vocabulary (<c>rateLimitExceeded</c>,
/// <c>backendError</c>, ...) is the same one BigQuery's REST envelope and job <c>errorResult</c> both
/// use, so <see cref="FromRest"/> and <see cref="FromJobError"/> share one classifier keyed primarily
/// by reason and falling back to the HTTP status when a job error carries no reason. Messages always
/// pass the redactor: BigQuery's own diagnostics echo request context that may carry a service-account
/// key fragment or a signed URL.</summary>
internal static class BqErrors
{
    /// <summary>Reasons the REST envelope and job <c>errorResult</c> both use for a service-side
    /// condition that may clear on its own: rate limiting and internal backend/job failures. Distinct
    /// from <c>quotaExceeded</c>, which is a hard ceiling a retry cannot outrun.</summary>
    public static bool IsTransientReason(string? reason) =>
        reason is "rateLimitExceeded" or "backendError" or "internalError" or "jobBackendError" or "jobInternalError";

    /// <summary>A failed REST call (job insert/get, table/dataset metadata, load/query job status).
    /// <paramref name="context"/> names what was being done ("creating staging table x") so the
    /// resulting message stands on its own in <c>run_results.json</c>.</summary>
    public static PzConnectorException FromRest(int status, string? reason, string? message, TimeSpan? retryAfter,
        BqRedactor redactor, string context) =>
        Classify(status, reason, message, retryAfter, redactor, context);

    /// <summary>A finished job's <c>errorResult</c>: no HTTP status of its own, so classification runs
    /// on <paramref name="reason"/> alone. <paramref name="purpose"/> plays the role <c>context</c>
    /// plays for <see cref="FromRest"/> -- it names what the job was for, and the result message
    /// always contains it.</summary>
    public static PzConnectorException FromJobError(string? reason, string? message, BqRedactor redactor, string purpose) =>
        Classify(status: 0, reason, message, retryAfter: null, redactor, purpose);

    /// <summary>A failed Storage Read API call. That transport is read-only, so its <c>NotFound</c>
    /// and <c>PermissionDenied</c> land on the 02xx read codes rather than the generic 04xx ones.</summary>
    public static PzConnectorException FromRpc(RpcException ex, BqRedactor redactor, string context)
    {
        // Status.Detail carries no nullable annotation on Grpc.Core.Api's Status, and a status built
        // with a null detail (as some interceptors and every default(Status) do) hands one back here.
        var detail = ex.Status.Detail;
        switch (ex.StatusCode)
        {
            case StatusCode.InvalidArgument when detail?.Contains("view", StringComparison.OrdinalIgnoreCase) == true:
                return NonTransient(BqCodes.Read_TableIsView, redactor,
                    $"{context}: {detail} -- the Storage API cannot read views; use `query: select * from <view>` instead", ex);

            case StatusCode.InvalidArgument:
                return NonTransient(BqCodes.Remote_InvalidQuery, redactor, $"{context}: {detail}", ex);

            case StatusCode.NotFound:
                return NonTransient(BqCodes.Read_TableNotFound, redactor, $"{context}: {detail}", ex);

            case StatusCode.PermissionDenied:
                return NonTransient(BqCodes.Read_PermissionDenied, redactor,
                    $"{context}: {detail} -- needs bigquery.readsessions.create and bigquery.tables.getData "
                    + "(roles/bigquery.dataViewer + roles/bigquery.readSessionUser)", ex);

            case StatusCode.Unauthenticated:
                return NonTransient(BqCodes.Remote_Unauthenticated, redactor,
                    $"{context}: {detail} -- check the service-account key or Application Default Credentials", ex);

            // FailedPrecondition is how an expired read session surfaces: the session's deadline
            // passed server-side, and a fresh CreateReadSession call recovers it, so it is transient
            // exactly like the transport-level codes.
            case StatusCode.Unavailable or StatusCode.ResourceExhausted or StatusCode.DeadlineExceeded
                or StatusCode.Aborted or StatusCode.Internal or StatusCode.Unknown or StatusCode.FailedPrecondition:
                return Transient(BqCodes.Remote_GrpcTransient, redactor, $"{context}: {ex.StatusCode}: {detail}", retryAfter: null, ex);

            default:
                return NonTransient(BqCodes.Remote_Other, redactor, $"{context}: {ex.StatusCode}: {detail}", ex);
        }
    }

    /// <summary>A client-side failure before any BigQuery response arrived at all -- DNS, refused
    /// connection, TLS handshake, a client-side timeout. <paramref name="ex"/> must not be an
    /// <see cref="OperationCanceledException"/>: the engine's own cancellation is the caller's to
    /// rethrow unwrapped, never to publish as a connector failure, so handing one here is a caller
    /// bug and throws loudly instead of silently mis-classifying it.</summary>
    public static PzConnectorException Wrap(Exception ex, BqRedactor redactor, string context)
    {
        if (ex is OperationCanceledException)
        {
            throw new InvalidOperationException(
                $"{nameof(BqErrors)}.{nameof(Wrap)} must not be called with {nameof(OperationCanceledException)} "
                + "-- the caller rethrows cancellation unwrapped.", ex);
        }

        return Transient(BqCodes.Remote_Transient, redactor, $"{context}: {ex.Message}", retryAfter: null, ex);
    }

    private static PzConnectorException Classify(int status, string? reason, string? message, TimeSpan? retryAfter,
        BqRedactor redactor, string context)
    {
        if (string.Equals(reason, "quotaExceeded", StringComparison.Ordinal))
        {
            return NonTransient(BqCodes.Remote_QuotaExceeded, redactor, $"{context}: {Describe(status, reason, message)}");
        }

        if (IsTransientReason(reason))
        {
            return Transient(BqCodes.Remote_TransientService, redactor, $"{context}: {Describe(status, reason, message)}", retryAfter);
        }

        if (string.Equals(reason, "accessDenied", StringComparison.Ordinal) || status == 403)
        {
            return NonTransient(BqCodes.Remote_PermissionDenied, redactor,
                $"{context}: {Describe(status, reason, message)} -- needs roles/bigquery.dataEditor + roles/bigquery.jobUser "
                + "for writes, or roles/bigquery.dataViewer + roles/bigquery.readSessionUser for reads");
        }

        if (string.Equals(reason, "notFound", StringComparison.Ordinal) || status == 404)
        {
            return NonTransient(BqCodes.Remote_NotFound, redactor, $"{context}: {Describe(status, reason, message)}");
        }

        if (status == 401)
        {
            return NonTransient(BqCodes.Remote_Unauthenticated, redactor,
                $"{context}: {Describe(status, reason, message)} -- check the service-account key or Application Default Credentials");
        }

        if (reason is "invalid" or "invalidQuery" || status == 400)
        {
            return NonTransient(BqCodes.Remote_InvalidQuery, redactor, $"{context}: {message ?? Describe(status, reason, message)}");
        }

        if (status == 429 || status is >= 500 and <= 599)
        {
            return Transient(BqCodes.Remote_TransientService, redactor, $"{context}: {Describe(status, reason, message)}", retryAfter);
        }

        return NonTransient(BqCodes.Remote_Other, redactor, $"{context}: {Describe(status, reason, message)}");
    }

    /// <summary>"HTTP 404 (notFound): no such table" for a REST/job status; "job error (notFound): ..."
    /// when <paramref name="status"/> is 0 -- a job's <c>errorResult</c> carries no HTTP status of its
    /// own, and printing "HTTP 0" would misdescribe it.</summary>
    private static string Describe(int status, string? reason, string? message)
    {
        var head = status > 0 ? $"HTTP {status}" : "job error";
        return (reason, message) switch
        {
            (null, null) => head,
            (not null, null) => $"{head} ({reason})",
            (null, not null) => $"{head}: {message}",
            _ => $"{head} ({reason}): {message}",
        };
    }

    // innerException defaults to null for the REST/job-error paths, which classify a parsed error
    // envelope rather than an in-flight exception; FromRpc and Wrap pass the exception they caught.
    private static PzConnectorException Transient(string code, BqRedactor redactor, string text, TimeSpan? retryAfter,
        Exception? innerException = null) =>
        new(BqCodes.Message(code, redactor, text), isTransient: true, retryAfter: retryAfter, innerException: innerException);

    private static PzConnectorException NonTransient(string code, BqRedactor redactor, string text, Exception? innerException = null) =>
        new(BqCodes.Message(code, redactor, text), isTransient: false, innerException: innerException);
}
