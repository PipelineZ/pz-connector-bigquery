using System.Globalization;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>One output's write: rows spool to NDJSON as they arrive, and every network call --
/// staging the spool via load jobs, then the one job that touches the target -- happens in
/// <see cref="CommitAsync"/>. <see cref="AbortAsync"/> only ever needs to drop the spool in practice
/// (staging is created nowhere but inside <see cref="CommitAsync"/>, and the engine never calls Abort
/// once Commit has been attempted), but still checks <see cref="_stagingCreated"/> for the same
/// defensive symmetry <see cref="CommitAsync"/>'s own <c>finally</c> block already applies.
///
/// <para>CLEANUP NEVER THROWS: every cleanup step (spool delete, staging drop) is individually
/// guarded and logged at Warning rather than allowed to propagate -- a temp-directory deletion
/// failure or a stray staging-drop error must never turn an already-committed write into a reported
/// failure (the engine would retry it, and in <c>append</c> mode a retry re-loads rows the target
/// already has), and must never mask whatever exception the commit itself actually raised.</para></summary>
internal sealed class BqWriteSession : ISinkWriteSession
{
    // Long enough to outlive a run that never gets to run this session's own cleanup (a crash, a
    // killed process) -- BigQuery drops the table on its own past this point even then -- short
    // enough that an interrupted run does not leave staged data sitting around indefinitely.
    private static readonly TimeSpan StagingTtl = TimeSpan.FromHours(6);

    private readonly BqConnectionConfig _cfg;
    private readonly BqRestClient _rest;
    private readonly BqRedactor _redactor;
    private readonly ILogger _logger;
    private readonly TimeProvider _time;
    private readonly TableRef _target;
    private readonly OutputSpec _spec;
    private readonly BqJsonRowWriter _rowWriter;
    private readonly IReadOnlyList<string> _columns;
    private readonly BqTableSchema _stagingSchema;
    private readonly BqTableSchema _targetWantedSchema;
    private readonly BqSpool _spool;

    private long _nextSequence;
    private long _rows;
    private long _batches;
    private TableRef? _stagingTable;
    private bool _stagingCreated;
    private State _state = State.Open;

    private enum State { Open, Committed, Aborted, Disposed }

    internal BqWriteSession(
        BqConnectionConfig cfg, BqRestClient rest, BqRedactor redactor, ILogger logger, TimeProvider time,
        TableRef target, OutputSpec spec, Schema schema, IReadOnlyList<string> columns,
        BqTableSchema stagingSchema, BqTableSchema targetWantedSchema, bool withSequence, BqSpool spool)
    {
        _cfg = cfg;
        _rest = rest;
        _redactor = redactor;
        _logger = logger;
        _time = time;
        _target = target;
        _spec = spec;
        _columns = columns;
        _stagingSchema = stagingSchema;
        _targetWantedSchema = targetWantedSchema;
        _spool = spool;
        _rowWriter = new BqJsonRowWriter(schema, withSequence);
    }

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen();
        // Written synchronously within the call, before any await -- the batch's buffers may be
        // pooled, off-heap native memory the engine reclaims the instant this call returns.
        _nextSequence = _rowWriter.Write(batch, _spool.Current, _nextSequence);
        _rows += batch.Length;
        _batches++;
        await _spool.RollIfNeededAsync().ConfigureAwait(false);
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureNotFinished();
        _state = State.Committed;

        var start = _time.GetTimestamp();
        var stagingDataset = _cfg.StagingDataset ?? new TableRef(_target.Project, _target.Dataset, "");
        var staging = new TableRef(stagingDataset.Project, stagingDataset.Dataset, $"pz_load_{Guid.NewGuid():N}");
        _stagingTable = staging;

        try
        {
            var files = await _spool.CloseAsync().ConfigureAwait(false);

            // Set before the call, not after it returns: a create that succeeded server-side but
            // whose response never arrived (a dropped connection) must still be dropped by the
            // finally below -- DeleteTableAsync already tolerates a staging table that, for
            // whatever reason, never actually got created.
            _stagingCreated = true;
            await CreateStagingTableAsync(staging, ct).ConfigureAwait(false);

            foreach (var file in files)
            {
                await LoadFileAsync(staging, file, ct).ConfigureAwait(false);
            }

            await RunTargetJobAsync(staging, ct).ConfigureAwait(false);

            _logger.LogDebug(
                "bigquery: output {Output}: committed {Rows} row(s) in {Batches} batch(es) over {Files} load job(s) in {ElapsedMs}ms",
                _spec.Output, _rows, _batches, files.Count, _time.GetElapsedTime(start).TotalMilliseconds);
            return new WriteResult(_rows, _batches);
        }
        finally
        {
            // Order does not matter here (unlike AbortAsync below): the spool file is already
            // closed by CloseAsync above by the time either cleanup step runs, so there is no open
            // handle for the directory delete to race against.
            if (_stagingCreated)
            {
                await TryDeleteStagingAsync(staging).ConfigureAwait(false);
            }

            TryDeleteSpool();
        }
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        if (_state == State.Committed)
        {
            throw new InvalidOperationException("AbortAsync after CommitAsync is not allowed");
        }

        if (_state == State.Aborted)
        {
            throw new InvalidOperationException("the session is already aborted");
        }

        _state = State.Aborted;

        // Order matters: the spool's current file may still be open (WriteBatchAsync never closes
        // it), and BqSpool.Delete() does not know to close it first -- deleting the directory out
        // from under an open handle leaks it everywhere and hard-fails on Windows. Closing it here
        // is itself guarded: a flush/close failure must not skip the staging drop that follows.
        await TryCloseSpoolAsync().ConfigureAwait(false);

        if (_stagingCreated && _stagingTable is { } staging)
        {
            await TryDeleteStagingAsync(staging).ConfigureAwait(false);
        }

        TryDeleteSpool();
    }

    public async ValueTask DisposeAsync()
    {
        if (_state == State.Open)
        {
            await AbortAsync(CancellationToken.None).ConfigureAwait(false);
        }

        _state = State.Disposed;
    }

    private void EnsureOpen()
    {
        if (_state != State.Open)
        {
            throw new InvalidOperationException($"the session is not open (state: {_state})");
        }
    }

    private void EnsureNotFinished()
    {
        if (_state == State.Committed)
        {
            throw new InvalidOperationException("the session is already committed");
        }

        if (_state == State.Aborted)
        {
            throw new InvalidOperationException("the session is aborted");
        }
    }

    // Best effort by construction, like BqSpool.Delete's own doc: a cleanup failure here must
    // never propagate out of Commit/AbortAsync (see the class doc) -- logged and swallowed, never
    // thrown. The spool's own directory path is not user data (a temp guid, not a config value or
    // row content), so it is safe to log.
    private async Task TryDeleteStagingAsync(TableRef staging)
    {
        try
        {
            await _rest.DeleteTableAsync(staging, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "bigquery: output {Output}: failed to drop staging table {Staging}", _spec.Output, staging.Quoted);
        }
    }

    private void TryDeleteSpool()
    {
        try
        {
            _spool.Delete();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "bigquery: output {Output}: failed to delete the spool directory {Dir}", _spec.Output, _spool.Dir);
        }
    }

    private async Task TryCloseSpoolAsync()
    {
        try
        {
            await _spool.CloseAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "bigquery: output {Output}: failed to close the spool file", _spec.Output);
        }
    }

    private async Task CreateStagingTableAsync(TableRef staging, CancellationToken ct)
    {
        var expiresAt = _time.GetUtcNow() + StagingTtl;
        var table = new BqTable(
            new BqTableReference(staging.Project, staging.Dataset, staging.Table),
            _stagingSchema,
            Type: null,
            ExpirationTime: expiresAt.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture));
        await _rest.InsertTableAsync(table, ct).ConfigureAwait(false);
    }

    private async Task LoadFileAsync(TableRef staging, string file, CancellationToken ct)
    {
        await using var stream = new FileStream(
            file, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, FileOptions.Asynchronous);
        var jobId = BqJob.NewJobId("load_staging");
        var job = BqLoadJob.Build(jobId, _cfg.Location, staging, _stagingSchema);
        var uploaded = await _rest.UploadLoadJobAsync(staging.Project, job, stream, stream.Length, ct).ConfigureAwait(false);
        await BqJob.WaitAsync(_rest, staging.Project, uploaded, job.JobReference?.Location, "load into staging",
            _time, _logger, ct).ConfigureAwait(false);
    }

    /// <summary>The one job that touches the target (<c>CommitAsync</c> step 4): <c>EnsureTargetAsync</c>
    /// decides whether the target already exists (and, if so, that its schema is compatible), then
    /// dispatches to the DML/query-destination job the mode and that answer call for.</summary>
    private async Task RunTargetJobAsync(TableRef staging, CancellationToken ct)
    {
        var targetExists = await EnsureTargetAsync(ct).ConfigureAwait(false);

        switch (_spec.Mode)
        {
            case "append" when targetExists:
                await RunDmlAsync(BqSql.Append(_target, staging, _columns), ct).ConfigureAwait(false);
                break;

            case "append":
                await RunCreatingQueryAsync(BqSql.Select(staging, _columns), "WRITE_APPEND", ct).ConfigureAwait(false);
                break;

            case "replace":
                await RunCreatingQueryAsync(BqSql.Select(staging, _columns), "WRITE_TRUNCATE", ct).ConfigureAwait(false);
                break;

            // A merge into a target that does not exist yet is plainly an insert of the
            // deduplicated final state -- MERGE's own DML syntax requires an existing target, so
            // creating one goes through the same query-destination mechanism append/replace use.
            case "merge" when !targetExists:
                await RunCreatingQueryAsync(BqSql.DedupedSelect(staging, _columns, _spec.Keys), "WRITE_APPEND", ct)
                    .ConfigureAwait(false);
                break;

            case "merge": // target exists
                await RunDmlAsync(BqSql.Merge(_target, staging, _columns, _spec.Keys), ct).ConfigureAwait(false);
                break;

            default:
                // BeginWriteAsync already refused any mode outside append/replace/merge (PZBQ0306)
                // -- reaching here would mean this switch fell behind that check, not that the
                // engine sent something unexpected. An explicit throw, rather than a `default:`
                // arm that silently ran the merge DML, keeps a future fourth mode from landing here
                // unnoticed.
                throw new InvalidOperationException(
                    $"unexpected write mode '{_spec.Mode}' reached RunTargetJobAsync -- BeginWriteAsync should have refused it");
        }
    }

    /// <summary><c>tables.get</c> on the target: missing (returns false) leaves creation to the
    /// caller's query-destination job; present checks the write schema against it
    /// (<see cref="BqTargetSchema.Diff"/>), refusing any mismatch with every offending column named
    /// (<c>PZBQ0304</c>) -- the evolve policy never reaches this method, since
    /// <c>BqSink.BeginWriteAsync</c> already refused it before any network call, before the spool
    /// even exists.</summary>
    private async Task<bool> EnsureTargetAsync(CancellationToken ct)
    {
        var existing = await _rest.GetTableAsync(_target, ct).ConfigureAwait(false);
        if (existing is null)
        {
            return false;
        }

        var existingSchema = existing.Schema ?? new BqTableSchema([]);
        var mismatches = BqTargetSchema.Diff(existingSchema, _targetWantedSchema);
        if (mismatches.Count > 0)
        {
            throw new PzConnectorException(
                BqCodes.Message(BqCodes.Write_SchemaMismatch, _redactor,
                    $"output '{_spec.Output}': the target table {_target.Quoted} does not match the write schema: "
                    + $"{string.Join("; ", mismatches)} -- align the target by hand, or drop it and let the sink recreate it"),
                isTransient: false);
        }

        return true;
    }

    private Task RunDmlAsync(string sql, CancellationToken ct)
    {
        var jobId = BqJob.NewJobId("commit_statement");
        var job = BqQueryJob.Build(jobId, _cfg.Location, sql, destination: null, writeDisposition: null, createDisposition: null);
        return BqJob.SubmitAndWaitAsync(_rest, _target.Project, job, "commit statement", _time, _logger, ct);
    }

    private Task RunCreatingQueryAsync(string sql, string writeDisposition, CancellationToken ct)
    {
        var jobId = BqJob.NewJobId("commit_statement");
        var job = BqQueryJob.Build(jobId, _cfg.Location, sql, _target, writeDisposition, "CREATE_IF_NEEDED");
        return BqJob.SubmitAndWaitAsync(_rest, _target.Project, job, "commit statement", _time, _logger, ct);
    }
}
