using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>Opens one write session per output. Every check here is offline (no network, no
/// filesystem -- the spool directory is not even computed until every check below has passed): mode
/// validity (ABI defense in depth -- the engine never sends anything else, but a directly-constructed
/// spec could), the unsupported <c>evolve</c> schema policy, merge keys against the write schema, the
/// reserved <c>_pz_seq</c> column name, the Arrow-to-BigQuery schema map, and the output's entity
/// name. Everything that touches the network -- staging, loading, and the one job that touches the
/// target -- happens inside <see cref="BqWriteSession.CommitAsync"/>.</summary>
internal sealed class BqSink(
    BqConnectionConfig cfg, BqRestClient rest, BqRedactor redactor, ILogger logger, TimeProvider time,
    long spoolRollBytes = 64 * 1024 * 1024) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        if (spec.Mode is not ("append" or "replace" or "merge"))
        {
            throw new PzConnectorException(
                BqCodes.Message(BqCodes.Write_BadWriteMode, redactor,
                    $"output '{spec.Output}': write mode '{spec.Mode}' is not supported here; use append, replace, or merge"),
                isTransient: false);
        }

        // Checked before any network call at all (and before the spool is even computed) -- an
        // unsupported policy is a config mistake, not something a wasted spool or staging table
        // should precede.
        if (string.Equals(spec.SchemaPolicy, "evolve", StringComparison.Ordinal))
        {
            throw new PzConnectorException(
                BqCodes.Message(BqCodes.Write_SchemaEvolveUnsupported, redactor,
                    $"output '{spec.Output}': schema evolution is not supported; use 'fail_on_change' and "
                    + "align the target table by hand, or drop it and let the sink recreate it"),
                isTransient: false);
        }

        if (string.Equals(spec.Mode, "merge", StringComparison.Ordinal))
        {
            if (spec.Keys.Count == 0)
            {
                throw new PzConnectorException(
                    BqCodes.Message(BqCodes.Write_MergeKeyMissing, redactor,
                        $"output '{spec.Output}': merge mode requires at least one merge key (write.keys)"),
                    isTransient: false);
            }

            var fieldNames = new HashSet<string>(schema.FieldsList.Select(f => f.Name), StringComparer.Ordinal);
            var missing = spec.Keys.Where(k => !fieldNames.Contains(k)).ToList();
            if (missing.Count > 0)
            {
                throw new PzConnectorException(
                    BqCodes.Message(BqCodes.Write_MergeKeyMissing, redactor,
                        $"output '{spec.Output}': merge key(s) {string.Join(", ", missing.Select(k => $"'{k}'"))} "
                        + "not present in the write schema"),
                    isTransient: false);
            }
        }

        if (schema.GetFieldByName(BqSchemaMap.SequenceColumn) is not null)
        {
            throw new PzConnectorException(
                BqCodes.Message(BqCodes.Write_ReservedColumnSeq, redactor,
                    $"output '{spec.Output}': column '{BqSchemaMap.SequenceColumn}' is reserved for merge-mode "
                    + "row ordering and cannot appear in the write schema"),
                isTransient: false);
        }

        var errors = new List<string>();
        var outputConfig = BqOutputConfig.Parse(spec, cfg, errors);
        if (outputConfig is null || errors.Count > 0)
        {
            throw new PzConnectorException(
                BqCodes.Message(BqCodes.Write_BadOutputOption, redactor, string.Join("; ", errors)), isTransient: false);
        }

        var withSequence = string.Equals(spec.Mode, "merge", StringComparison.Ordinal);
        var stagingSchema = BqSchemaMap.ToBigQuery(schema, withSequence, spec.Output);
        var targetWantedSchema = BqSchemaMap.ToBigQuery(schema, withSequence: false, spec.Output);
        var columns = schema.FieldsList.Select(f => f.Name).ToArray();

        var dir = Path.Combine(Path.GetTempPath(), "pz-bigquery", Guid.NewGuid().ToString("N"));
        var spool = new BqSpool(dir, spoolRollBytes);

        ISinkWriteSession session = new BqWriteSession(
            cfg, rest, redactor, logger, time, outputConfig.Target, spec, schema, columns,
            stagingSchema, targetWantedSchema, withSequence, spool);
        return ValueTask.FromResult(session);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
