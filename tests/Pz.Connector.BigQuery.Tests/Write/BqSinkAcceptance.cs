using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>TestKit sink contract against the emulator. Fresh table names per instance (xunit
/// constructs one per fact) inside dataset <see cref="BqFixture.Dataset"/>, so facts never share
/// state.</summary>
[Collection("bigquery")]
[Trait("Category", "Docker")]
public sealed class BqSinkAcceptance : SinkConnectorAcceptanceTests
{
    private readonly BqFixture _bq;
    private readonly string _append = BqFixture.NewName("append");
    private readonly string _merge = BqFixture.NewName("merge");
    private readonly string _replace = BqFixture.NewName("replace");

    public BqSinkAcceptance(BqFixture bq)
    {
        _bq = bq;
        DockerFacts.SkipUnlessDocker();
    }

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    // Merge_upserts_by_keys/Merge_is_idempotent each commit twice against the SAME merge target, so
    // their second commit hits an ALREADY-EXISTING target and dispatches to BqSql.Merge's DML --
    // which this specific emulator image (goccy/bigquery-emulator 0.8.1) refuses outright: its MERGE
    // parser rejects any `using (...)` source that is not a bare table reference ("MERGE: source
    // must be a single-table reference, got *googlesql.ResolvedProjectScan"), which is exactly the
    // windowed dedup subquery BqSql.Merge always builds -- a shape real BigQuery's own documented
    // MERGE syntax explicitly allows.
    // Replace_mode_overwrites_the_prior_commit needs its second commit's WRITE_TRUNCATE to actually
    // truncate; this emulator does not honor that disposition on a query-destination job (it behaves
    // like WRITE_APPEND).
    // BqSinkBehaviorTests' class doc has the full detail and the docker-level facts that prove each
    // failure/behavior is classified correctly instead of silently routed around; BqSqlTests already
    // pins BqSql.Merge's exact (real-BigQuery-correct) SQL shape, and BqWriteSessionTests proves both
    // the dispatch to it for an existing merge target and the WRITE_TRUNCATE disposition for replace,
    // without needing this emulator to execute or honor either.
    // If BqFixture.Image is ever bumped past 0.8.1 and no longer exhibits either behaviour, remove
    // this exclusion and let these three facts run for real -- they are the correct, real-BigQuery
    // proof this exclusion stands in for, not a permanent substitute for it.
    protected override bool ShouldRun(string fact) =>
        fact is not ("Merge_upserts_by_keys" or "Merge_is_idempotent" or "Replace_mode_overwrites_the_prior_commit");

    protected override ISinkConnector CreateSink() => new BqConnector();

    protected override ConnectorConfig ValidConfig => new(_bq.ConnectionConfig());

    protected override OutputSpec SmallOutput =>
        new("bigquery", $"{BqFixture.Dataset}.{_append}", "append", "fail_on_change", new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput =>
        new OutputSpec("bigquery", $"{BqFixture.Dataset}.{_merge}", "merge", "fail_on_change", new Dictionary<string, object?>())
        {
            Keys = ["id"],
        };

    protected override Task ResetMergeTargetAsync() => _bq.DeleteTableAsync(_merge);

    protected override OutputSpec? ReplaceOutput =>
        new("bigquery", $"{BqFixture.Dataset}.{_replace}", "replace", "fail_on_change", new Dictionary<string, object?>());

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var name = spec.Output[(spec.Output.IndexOf('.') + 1)..];
        if (!await _bq.TableExistsAsync(name))
        {
            return [];
        }

        var rows = await _bq.QueryAsync($"select id, name from {BqFixture.Dataset}.{name} order by id");
        if (rows.Count == 0)
        {
            return [];
        }

        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        foreach (var row in rows)
        {
            ids.Append(long.Parse(row.GetProperty("id").GetString()!, System.Globalization.CultureInfo.InvariantCulture));
            names.Append(row.GetProperty("name").GetString()!);
        }

        var schema = new Schema([new Field("id", Int64Type.Default, false), new Field("name", StringType.Default, false)], null);
        return [new RecordBatch(schema, [ids.Build(), names.Build()], rows.Count)];
    }
}
