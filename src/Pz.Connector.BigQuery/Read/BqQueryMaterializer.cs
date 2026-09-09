using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.Extensions.Logging;

namespace Pz.Connector.BigQuery;

/// <summary>Lands a <c>query:</c> read's SQL into a table under <see cref="BqConnectionConfig.StagingDataset"/>
/// via a query job, then hands the destination back so the rest of the source treats it like any
/// other table -- the Storage Read API cannot stream an arbitrary query's results directly. Results
/// are cached by the trimmed query text: <see cref="ConcurrentDictionary{TKey,TValue}.GetOrAdd"/> of
/// a lazily started task means two callers materializing the same query in one run (a source's own
/// <c>GetSchemaAsync</c> then <c>PlanReadAsync</c>) share a single query job rather than each
/// submitting their own. A faulted materialization is evicted from the cache on its way out so a
/// later call (a retry, say) resubmits rather than replaying the same failure forever.</summary>
internal sealed class BqQueryMaterializer : IAsyncDisposable
{
    // Long enough to outlive a run that never gets to call DisposeAsync (a crash, a killed process) --
    // the emulator/BigQuery drops the table on its own past this point even then -- short enough that
    // an interrupted run does not leave staged data sitting around indefinitely.
    private static readonly TimeSpan Ttl = TimeSpan.FromHours(6);

    private readonly BqRestClient _rest;
    private readonly BqConnectionConfig _cfg;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, Task<TableRef>> _cache = new(StringComparer.Ordinal);

    public BqQueryMaterializer(BqRestClient rest, BqConnectionConfig cfg, TimeProvider time, ILogger logger)
    {
        _rest = rest;
        _cfg = cfg;
        _time = time;
        _logger = logger;
    }

    public async Task<TableRef> MaterializeAsync(string query, CancellationToken ct)
    {
        var key = query.Trim();
        var task = _cache.GetOrAdd(key, _ => MaterializeCoreAsync(key, ct));
        try
        {
            return await task.ConfigureAwait(false);
        }
        catch
        {
            // Only removes the exact faulted task this call observed -- a racing caller that already
            // replaced it with a fresh attempt (unlikely, but not impossible between the throw above
            // and this catch) must not have its in-flight task yanked out from under it.
            _cache.TryRemove(new KeyValuePair<string, Task<TableRef>>(key, task));
            throw;
        }
    }

    /// <summary>Drops every table this instance materialized -- a 404 (already gone) is tolerated by
    /// <see cref="BqRestClient.DeleteTableAsync"/> itself; any other failure is logged, never thrown,
    /// so a staging cleanup problem never masks the run's real outcome or blocks the source from
    /// disposing.</summary>
    public async ValueTask DisposeAsync()
    {
        foreach (var (_, task) in _cache)
        {
            if (!task.IsCompletedSuccessfully)
            {
                continue;
            }

            var table = task.Result;
            try
            {
                await _rest.DeleteTableAsync(table, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "bigquery: failed to drop materialized query table {Table}", table.Quoted);
            }
        }
    }

    private async Task<TableRef> MaterializeCoreAsync(string query, CancellationToken ct)
    {
        // ResolveTable's caller (BqSource) never reaches this method without first parsing the
        // dataset config through BqDatasetConfig.Parse, which itself refuses 'query:' with no
        // staging_dataset (PZBQ0105) -- this null-check exists only to fail loudly rather than send a
        // malformed request if that invariant is ever broken by a future caller.
        var staging = _cfg.StagingDataset
            ?? throw new InvalidOperationException("MaterializeAsync requires a resolved staging_dataset; the caller must validate this first");

        var destination = new TableRef(staging.Project, staging.Dataset, $"pz_query_{Guid.NewGuid():N}");
        var sql = $"select * from (\n{StripTrailingSemicolon(query)}\n)";

        var jobId = BqJob.NewJobId("query");
        var job = BqQueryJob.Build(jobId, _cfg.Location, sql, destination, "WRITE_TRUNCATE", "CREATE_IF_NEEDED");

        var start = _time.GetTimestamp();
        await BqJob.SubmitAndWaitAsync(_rest, destination.Project, job, "materialize query", _time, _logger, ct).ConfigureAwait(false);
        _logger.LogDebug("bigquery: materialized query into {Table} via job {JobId} in {ElapsedMs}ms",
            destination.Quoted, jobId, _time.GetElapsedTime(start).TotalMilliseconds);

        var expiresAt = _time.GetUtcNow() + Ttl;
        var patch = new BqTablePatch(ExpirationTime: expiresAt.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture));
        await _rest.PatchTableAsync(destination, patch, ct).ConfigureAwait(false);

        return destination;
    }

    // The trailing ';' a user's YAML-embedded query often carries (copy-pasted from a SQL editor) is
    // not valid inside "select * from (...)" -- stripped here rather than pushed onto every caller.
    private static string StripTrailingSemicolon(string query)
    {
        var trimmed = query.TrimEnd();
        return trimmed.EndsWith(';') ? trimmed[..^1].TrimEnd() : trimmed;
    }
}
