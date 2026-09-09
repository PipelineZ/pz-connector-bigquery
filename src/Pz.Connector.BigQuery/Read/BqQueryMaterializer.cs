using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.Extensions.Logging;

namespace Pz.Connector.BigQuery;

/// <summary>Lands a <c>query:</c> read's SQL into a table under <see cref="BqConnectionConfig.StagingDataset"/>
/// via a query job, then hands the destination back so the rest of the source treats it like any
/// other table -- the Storage Read API cannot stream an arbitrary query's results directly. Results
/// are cached by the trimmed query text in a <c>Lazy&lt;Task&lt;TableRef&gt;&gt;</c> per key: two
/// callers racing to materialize the same query in one run (a source's own <c>GetSchemaAsync</c> then
/// <c>PlanReadAsync</c>, called concurrently) must submit exactly one query job between them, never
/// two -- a plain <c>Task</c> in the dictionary is not enough for that, since
/// <see cref="ConcurrentDictionary{TKey,TValue}.GetOrAdd"/> may invoke a losing caller's value
/// factory before discarding its result, and an async method's factory already started the real job
/// submission by the time it returns a <c>Task</c>. Wrapping the task in
/// <c>Lazy&lt;T&gt;(LazyThreadSafetyMode.ExecutionAndPublication)</c> defers that submission until
/// <c>.Value</c> is actually read, and only the one <see cref="Lazy{T}"/> instance <c>GetOrAdd</c>
/// ends up publishing ever has its value factory run -- so exactly one job is submitted regardless of
/// how many callers raced to get here.
///
/// <para>The shared body runs on this instance's own lifetime token, never a caller's: two callers
/// racing to materialize the same query carry two different <see cref="CancellationToken"/>s (the PCP
/// host issues a fresh one per RPC), and cancelling one caller's own operation must not cancel or
/// fault the materialization the other is still waiting on. Each caller instead layers its own token
/// on top with <see cref="Task.WaitAsync(CancellationToken)"/>, so its own cancellation only ends its
/// own wait. A materialization that itself faults (or is cancelled by this instance's own lifetime
/// token, in <see cref="DisposeAsync"/>) is evicted from the cache so a later call -- a retry, say --
/// resubmits rather than replaying the same failure forever; a caller whose own token merely stopped
/// its own wait leaves the shared entry alone.</para></summary>
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
    private readonly ConcurrentDictionary<string, Lazy<Task<TableRef>>> _cache = new(StringComparer.Ordinal);

    // Every destination this instance's query jobs have actually created, recorded the moment each
    // one's query job reaches DONE -- before the expiration patch that follows. Tracked independently
    // of _cache so a patch failure (a transient 5xx, a 403 on bigquery.tables.update) or a
    // cancellation past that point still leaves DisposeAsync a record to drop the table by; _cache
    // only ever answers "what is the table for this query text", not "what tables exist to clean up".
    // A ConcurrentDictionary used as a set -- the value is never read.
    private readonly ConcurrentDictionary<TableRef, byte> _created = new();

    // Cancelled in DisposeAsync so a materialization still running when the instance is disposed
    // (an abandoned run, a crash-adjacent shutdown) is not awaited forever; never a caller's own
    // token -- see the type doc.
    private readonly CancellationTokenSource _lifetime = new();

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
        var lazy = _cache.GetOrAdd(key,
            _ => new Lazy<Task<TableRef>>(() => MaterializeCoreAsync(key, _lifetime.Token), LazyThreadSafetyMode.ExecutionAndPublication));
        try
        {
            return await lazy.Value.WaitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            // WaitAsync throws for two different reasons that must be told apart: this caller's own
            // ct firing (the shared materialization is still running, or will still complete/fail for
            // whichever other caller is also awaiting it -- the cache entry must stay), versus the
            // shared task itself having faulted or been cancelled (by this instance's own lifetime
            // token in DisposeAsync), which really is a dead entry that must not be replayed forever.
            // Only removes the exact Lazy this call observed -- a racing caller that already replaced
            // it with a fresh attempt (unlikely, but not impossible) must not have its in-flight entry
            // yanked out from under it.
            if (lazy.Value.IsFaulted || lazy.Value.IsCanceled)
            {
                _cache.TryRemove(new KeyValuePair<string, Lazy<Task<TableRef>>>(key, lazy));
            }

            throw;
        }
    }

    /// <summary>Drops every table this instance's query jobs actually created, regardless of whether
    /// its materialization went on to succeed -- a table recorded in the created-tables set is a table
    /// that exists in BigQuery right now, full stop. A 404 (already gone) is tolerated by
    /// <see cref="BqRestClient.DeleteTableAsync"/> itself; any other failure is logged, never thrown,
    /// so a staging cleanup problem never masks the run's real outcome or blocks the source from
    /// disposing.</summary>
    public async ValueTask DisposeAsync()
    {
        await _lifetime.CancelAsync().ConfigureAwait(false);
        _lifetime.Dispose();

        foreach (var table in _created.Keys)
        {
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

        // The table exists in BigQuery from this point on, no matter what happens next -- recorded
        // before the expiration patch below so a failed or cancelled patch still leaves DisposeAsync a
        // name to drop it by.
        _created[destination] = 0;

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
