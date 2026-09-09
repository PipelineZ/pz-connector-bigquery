using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>Offline coverage (no docker, no network) for
/// <c>BqSource.CreateSessionAsync</c>'s view/not-view disambiguation: <c>tables.get</c> must be the
/// sole authority for <c>PZBQ0204</c>, never <see cref="BqErrors.FromRpc"/>'s own "does the gRPC
/// detail text mention 'view'" text heuristic (<c>BqErrors.cs</c>, the <c>InvalidArgument</c>
/// <c>when</c> arm). <see cref="StubFactory"/> stands in for the real <c>BigQueryReadClient</c> call
/// (dialing nothing), and a <see cref="FakeHandler"/>-backed <see cref="BqRestClient"/> stands in for
/// the real <c>tables.get</c> response -- exactly the two seams a live emulator would otherwise
/// exercise for this one code path.</summary>
public sealed class BqSourceViewDisambiguationTests
{
    private const string Project = "proj";
    private const string Dataset = "d";
    private const string Table = "t";

    private static BqConnectionConfig Config() => new(
        Project, BqAuthKind.None, null, null, null, null, BqConnectionConfig.DefaultRestBase, null, false, BqRedactor.None);

    /// <summary>Throws exactly what <see cref="BqReadSessionFactory.CreateAsync"/> itself throws for a
    /// failed <c>CreateReadSession</c> call -- <see cref="BqErrors.FromRpc"/> applied to
    /// <paramref name="toThrow"/> -- without dialing any gRPC channel. The only member this test needs
    /// stubbed is <see cref="BqReadSessionFactory.CreateAsync"/>; <c>ClientAsync</c>/<c>ReadRowsAsync</c>
    /// are never reached because <see cref="BqSource.CreateSessionAsync"/> fails before any partition
    /// is read.</summary>
    private sealed class StubFactory(RpcException toThrow) : BqReadSessionFactory(Config(), null, BqRedactor.None)
    {
        public override Task<BqSessionInfo> CreateAsync(
            TableRef table, IReadOnlyList<string>? columns, string? restriction, int maxStreams, CancellationToken ct) =>
            Task.FromException<BqSessionInfo>(
                BqErrors.FromRpc(toThrow, BqRedactor.None, $"creating a read session for {Dataset}.{Table}"));
    }

    private static BqRestClient RestClientReturning(string tableType)
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, $"/bigquery/v2/projects/{Project}/datasets/{Dataset}/tables/{Table}", 200,
            $$"""{"tableReference":{"projectId":"{{Project}}","datasetId":"{{Dataset}}","tableId":"{{Table}}"},"type":"{{tableType}}"}""");
        return new BqRestClient(new HttpClient(handler), Config(), BqRedactor.None, NullLogger.Instance,
            _ => Task.FromResult<string?>(null));
    }

    private static async Task<PzConnectorException> GetSchemaThrowsAsync(RpcException rpc, string tableType)
    {
        var source = new BqSource(Config(), RestClientReturning(tableType), new StubFactory(rpc), BqRedactor.None,
            NullLogger.Instance, TimeProvider.System);
        var spec = new DatasetSpec("bigquery", $"{Dataset}.{Table}", new Dictionary<string, object?>());

        return await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(spec, CancellationToken.None));
    }

    [Fact]
    public async Task Heuristic_false_positive_is_corrected_to_the_generic_code_when_tables_get_says_TABLE()
    {
        // The detail text coincidentally contains "view" (e.g. quoting an unrelated column or
        // message named that), which is exactly what BqErrors.FromRpc's heuristic keys on -- so the
        // exception BqReadSessionFactory.CreateAsync throws already carries PZBQ0204 before
        // BqSource.CreateSessionAsync ever gets a chance to check tables.get.
        var rpc = new RpcException(new Status(StatusCode.InvalidArgument, "column 'overview' not found"));

        var ex = await GetSchemaThrowsAsync(rpc, tableType: "TABLE");

        // Every message here starts with the fixed "bigquery:" prefix, which itself contains "query:"
        // as a substring (bigQUERY:) -- so the "next step" text this fact actually cares about is the
        // more specific "select * from" phrase the view-refusal message alone carries.
        Assert.DoesNotContain("PZBQ0204", ex.Message);
        Assert.Contains("PZBQ0407", ex.Message);
        Assert.DoesNotContain("select * from", ex.Message);
    }

    [Fact]
    public async Task Heuristic_true_positive_still_reports_PZBQ0204_when_tables_get_confirms_VIEW()
    {
        var rpc = new RpcException(new Status(StatusCode.InvalidArgument, "cannot read from a view"));

        var ex = await GetSchemaThrowsAsync(rpc, tableType: "VIEW");

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0204", ex.Message);
        Assert.Contains("query: select * from", ex.Message);
    }

    [Fact]
    public async Task Heuristic_false_negative_is_still_caught_via_tables_get_when_it_confirms_VIEW()
    {
        // No "view" anywhere in the detail text -- the heuristic would have guessed wrong on its own,
        // but tables.get is the actual authority, so PZBQ0204 must still be reported.
        var rpc = new RpcException(new Status(StatusCode.InvalidArgument, "unsupported read shape"));

        var ex = await GetSchemaThrowsAsync(rpc, tableType: "VIEW");

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0204", ex.Message);
        Assert.Contains("query: select * from", ex.Message);
    }
}
