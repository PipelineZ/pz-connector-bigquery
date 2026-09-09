using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>A BigQuery table read through the Storage Read API. Schema resolution
/// (<see cref="GetSchemaAsync"/>) and planning (<see cref="PlanReadAsync"/>) both need the session's
/// declared Arrow schema, so a resolved schema is cached per <c>(spec.Dataset, table)</c> -- a dataset
/// read more than once in the same run (the determinism/re-plan facts in the acceptance suite, or a
/// pipeline that references the same dataset from two nodes) reuses it rather than opening a second
/// session purely to learn the same schema again.
///
/// <para>Query-mode reads (<c>query:</c> instead of <c>entity:</c>) are not implemented yet -- landing a
/// query's results into <c>staging_dataset</c> before the Storage Read API can stream them back is a
/// later addition; both entry points throw <see cref="NotImplementedException"/> for that case.</para></summary>
internal sealed class BqSource : ISource
{
    private readonly BqConnectionConfig _cfg;
    private readonly BqRestClient _rest;
    private readonly BqReadSessionFactory _factory;
    private readonly BqRedactor _redactor;
    private readonly ILogger _logger;
    private readonly TimeProvider _time;
    private readonly ConcurrentDictionary<(string Dataset, string Table), DatasetSchema> _schemaCache = new();

    public BqSource(BqConnectionConfig cfg, BqRestClient rest, BqReadSessionFactory factory, BqRedactor redactor,
        ILogger logger, TimeProvider time)
    {
        _cfg = cfg;
        _rest = rest;
        _factory = factory;
        _redactor = redactor;
        _logger = logger;
        _time = time;
    }

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var table = ResolveTable(spec);
        return await ResolveSchemaAsync(spec, table, ct).ConfigureAwait(false);
    }

    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var table = ResolveTable(spec);
        var schema = await ResolveSchemaAsync(spec, table, ct).ConfigureAwait(false);
        var config = ParseDatasetConfig(spec);

        var restriction = BqRowRestriction.Build(hints, spec, schema.Schema, _redactor);
        var start = _time.GetTimestamp();
        var sessionInfo = await CreateSessionAsync(table, hints.Columns, restriction, config.Streams, ct).ConfigureAwait(false);
        _logger.LogDebug("bigquery: planned dataset {Dataset} into {StreamCount} stream(s) in {ElapsedMs}ms",
            spec.Dataset, sessionInfo.StreamNames.Count, _time.GetElapsedTime(start).TotalMilliseconds);

        return sessionInfo.StreamNames
            .Select(name => (IDatasetPartition)new BqPartition(sessionInfo.SerializedSchema, name, _factory, _redactor, table))
            .ToList();
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async Task<DatasetSchema> ResolveSchemaAsync(DatasetSpec spec, TableRef table, CancellationToken ct)
    {
        var key = (spec.Dataset, table.ResourcePath);
        if (_schemaCache.TryGetValue(key, out var cached))
        {
            return cached;
        }

        var sessionInfo = await CreateSessionAsync(table, columns: null, restriction: null, maxStreams: 0, ct).ConfigureAwait(false);
        var schema = new DatasetSchema(new BqArrowStreamDecoder(sessionInfo.SerializedSchema).ReadSchema());
        _schemaCache[key] = schema;
        return schema;
    }

    private TableRef ResolveTable(DatasetSpec spec)
    {
        var config = ParseDatasetConfig(spec);
        if (config.Query is not null)
        {
            throw new NotImplementedException("bigquery 'query:' reads are not implemented yet; use 'entity:' to read a table directly");
        }

        return config.Table!.Value;
    }

    private BqDatasetConfig ParseDatasetConfig(DatasetSpec spec)
    {
        var errors = new List<string>();
        return BqDatasetConfig.Parse(spec, _cfg, errors)
            ?? throw new PzConnectorException(
                BqCodes.Message(BqCodes.Read_BadDatasetOption, _redactor, string.Join("; ", errors)), isTransient: false);
    }

    /// <summary>Tries <see cref="BqReadSessionFactory.CreateAsync"/> as-is first -- the happy path
    /// never touches REST. Only when that fails with <c>InvalidArgument</c> does this fall back to
    /// <c>tables.get</c>: the Storage Read API refuses to read a view with that status, but its error
    /// detail text is not something this connector controls or trusts across BigQuery and every
    /// emulator that speaks its protocol, so the view/not-view distinction is made by asking the REST
    /// API what the resource actually is instead of pattern-matching a message.</summary>
    private async Task<BqSessionInfo> CreateSessionAsync(
        TableRef table, IReadOnlyList<string>? columns, string? restriction, int maxStreams, CancellationToken ct)
    {
        try
        {
            return await _factory.CreateAsync(table, columns, restriction, maxStreams, ct).ConfigureAwait(false);
        }
        catch (PzConnectorException ex) when (ex.InnerException is RpcException { StatusCode: StatusCode.InvalidArgument })
        {
            var meta = await _rest.GetTableAsync(table, ct).ConfigureAwait(false);
            if (meta?.Type == "VIEW")
            {
                throw new PzConnectorException(
                    BqCodes.Message(BqCodes.Read_TableIsView, _redactor,
                        $"reading {table.Dataset}.{table.Table}: the Storage Read API cannot read views -- "
                        + $"use `query: select * from {table.Quoted}` instead"),
                    isTransient: false, innerException: ex);
            }

            throw;
        }
    }
}
