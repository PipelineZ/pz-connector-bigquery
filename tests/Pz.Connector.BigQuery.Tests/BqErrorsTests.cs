using Grpc.Core;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqErrorsTests
{
    [Theory]
    [InlineData(429, null, true)]
    [InlineData(500, null, true)]
    [InlineData(502, null, true)]
    [InlineData(503, null, true)]
    [InlineData(504, null, true)]
    [InlineData(403, "rateLimitExceeded", true)]
    [InlineData(403, "quotaExceeded", false)]
    [InlineData(401, null, false)]
    [InlineData(403, "accessDenied", false)]
    [InlineData(404, null, false)]
    [InlineData(400, "invalidQuery", false)]
    [InlineData(418, null, false)]
    public void FromRest_classifies_transience_per_the_status_reason_table(int status, string? reason, bool transient)
    {
        var ex = BqErrors.FromRest(status, reason, "detail", null, BqRedactor.None, "ctx");

        Assert.Equal(transient, ex.IsTransient);
    }

    [Fact]
    public void FromRest_429_honors_retry_after()
    {
        var ex = BqErrors.FromRest(429, "rateLimitExceeded", "slow down", TimeSpan.FromSeconds(5), BqRedactor.None, "loading orders");

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(5), ex.RetryAfter);
        Assert.StartsWith("bigquery: PZBQ0402: ", ex.Message);
    }

    [Fact]
    public void FromRest_401_is_unauthenticated_with_a_credential_hint()
    {
        var ex = BqErrors.FromRest(401, null, "invalid credentials", null, BqRedactor.None, "opening connection");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0404: ", ex.Message);
        Assert.Contains("service-account key", ex.Message);
    }

    [Fact]
    public void FromRest_403_access_denied_names_the_roles()
    {
        var ex = BqErrors.FromRest(403, "accessDenied", "forbidden", null, BqRedactor.None, "writing sink");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0405: ", ex.Message);
        Assert.Contains("roles/bigquery", ex.Message);
    }

    [Fact]
    public void FromRest_404_names_the_context()
    {
        var ex = BqErrors.FromRest(404, null, "table not found", null, BqRedactor.None, "reading dataset 'orders'");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0406: ", ex.Message);
        Assert.Contains("reading dataset 'orders'", ex.Message);
    }

    [Fact]
    public void FromRest_400_invalid_query_carries_the_message_verbatim()
    {
        var ex = BqErrors.FromRest(400, "invalidQuery", "Syntax error at [3:5]", null, BqRedactor.None, "compiling pipeline");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0407: ", ex.Message);
        Assert.Contains("Syntax error at [3:5]", ex.Message);
    }

    [Fact]
    public void FromRest_unknown_status_is_non_transient_and_names_the_status_line()
    {
        var ex = BqErrors.FromRest(418, null, "I'm a teapot", null, BqRedactor.None, "ctx");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0409: ", ex.Message);
        Assert.Contains("418", ex.Message);
    }

    [Fact]
    public void FromJobError_uses_the_reason_table_and_names_the_purpose()
    {
        var ex = BqErrors.FromJobError("notFound", "table x not found", BqRedactor.None, "dropping staging table x");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0406: ", ex.Message);
        Assert.Contains("dropping staging table x", ex.Message);
    }

    [Fact]
    public void FromJobError_transient_reason_is_transient()
    {
        var ex = BqErrors.FromJobError("backendError", "internal", BqRedactor.None, "loading job");

        Assert.True(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0402: ", ex.Message);
    }

    [Theory]
    [InlineData(StatusCode.Unavailable, true)]
    [InlineData(StatusCode.ResourceExhausted, true)]
    [InlineData(StatusCode.DeadlineExceeded, true)]
    [InlineData(StatusCode.Aborted, true)]
    [InlineData(StatusCode.Internal, true)]
    [InlineData(StatusCode.Unknown, true)]
    [InlineData(StatusCode.FailedPrecondition, true)]
    [InlineData(StatusCode.NotFound, false)]
    [InlineData(StatusCode.PermissionDenied, false)]
    [InlineData(StatusCode.Unauthenticated, false)]
    public void FromRpc_classifies_transience(StatusCode code, bool transient)
    {
        var ex = BqErrors.FromRpc(new RpcException(new Status(code, "boom")), BqRedactor.None, "ctx");

        Assert.Equal(transient, ex.IsTransient);
    }

    [Fact]
    public void FromRpc_invalid_argument_naming_a_view_hints_the_query_workaround()
    {
        var original = new RpcException(new Status(StatusCode.InvalidArgument, "table 'x' is a VIEW, not queryable via Storage API"));
        var ex = BqErrors.FromRpc(original, BqRedactor.None, "reading 'x'");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0204: ", ex.Message);
        Assert.Contains("query:", ex.Message);
        Assert.Same(original, ex.InnerException);
    }

    [Fact]
    public void FromRpc_other_invalid_argument_is_the_generic_invalid_query_code()
    {
        var original = new RpcException(new Status(StatusCode.InvalidArgument, "bad filter"));
        var ex = BqErrors.FromRpc(original, BqRedactor.None, "ctx");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0407: ", ex.Message);
        Assert.Same(original, ex.InnerException);
    }

    [Fact]
    public void FromRpc_invalid_argument_with_a_null_detail_does_not_throw_and_falls_back_to_invalid_query()
    {
        // Status.Detail carries no nullable annotation; a status built with a null detail (a bare
        // default(Status), or an interceptor that never set one) must not crash the "is this a view"
        // guard, which used to call .Contains directly on a possibly-null string.
        var original = new RpcException(new Status(StatusCode.InvalidArgument, null!));
        var ex = BqErrors.FromRpc(original, BqRedactor.None, "ctx");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0407: ", ex.Message);
        Assert.Same(original, ex.InnerException);
    }

    [Fact]
    public void FromRpc_transient_status_with_a_null_detail_does_not_throw()
    {
        var original = new RpcException(new Status(StatusCode.Unavailable, null!));
        var ex = BqErrors.FromRpc(original, BqRedactor.None, "ctx");

        Assert.True(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0408: ", ex.Message);
        Assert.Same(original, ex.InnerException);
    }

    [Fact]
    public void FromRpc_not_found_is_the_read_table_not_found_code()
    {
        var original = new RpcException(new Status(StatusCode.NotFound, "no such table"));
        var ex = BqErrors.FromRpc(original, BqRedactor.None, "ctx");

        Assert.StartsWith("bigquery: PZBQ0205: ", ex.Message);
        Assert.Same(original, ex.InnerException);
    }

    [Fact]
    public void FromRpc_permission_denied_names_the_read_session_permissions()
    {
        var original = new RpcException(new Status(StatusCode.PermissionDenied, "denied"));
        var ex = BqErrors.FromRpc(original, BqRedactor.None, "ctx");

        Assert.StartsWith("bigquery: PZBQ0206: ", ex.Message);
        Assert.Contains("bigquery.readsessions.create", ex.Message);
        Assert.Same(original, ex.InnerException);
    }

    [Fact]
    public void FromRpc_unauthenticated_is_the_remote_unauthenticated_code()
    {
        var original = new RpcException(new Status(StatusCode.Unauthenticated, "no token"));
        var ex = BqErrors.FromRpc(original, BqRedactor.None, "ctx");

        Assert.False(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0404: ", ex.Message);
        Assert.Same(original, ex.InnerException);
    }

    [Fact]
    public void Wrap_transport_failure_is_transient()
    {
        var original = new HttpRequestException("refused");
        var ex = BqErrors.Wrap(original, BqRedactor.None, "ctx");

        Assert.True(ex.IsTransient);
        Assert.StartsWith("bigquery: PZBQ0401: ", ex.Message);
        Assert.Contains("refused", ex.Message);
        Assert.Same(original, ex.InnerException);
    }

    [Fact]
    public void Wrap_rejects_operation_canceled_loudly()
    {
        Assert.Throws<InvalidOperationException>(() => BqErrors.Wrap(new OperationCanceledException(), BqRedactor.None, "ctx"));
    }
}
