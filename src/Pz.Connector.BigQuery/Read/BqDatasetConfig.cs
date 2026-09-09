using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>A dataset's read options, parsed once at compile/open time. Exactly one of
/// <see cref="Table"/>/<see cref="Query"/> is set: an <c>entity:</c> read names a table directly,
/// a <c>query:</c> read runs GoogleSQL and lands its results in <c>staging_dataset</c> before the
/// Storage Read API streams them back -- BigQuery cannot stream an arbitrary query's results
/// directly.</summary>
internal sealed record BqDatasetConfig(TableRef? Table, string? Query, int Streams)
{
    private const int MinStreams = 1;
    private const int MaxStreams = 64;
    private const int DefaultStreams = 4;

    private static readonly string[] KnownOptions = ["entity", "query", "streams"];

    public static BqDatasetConfig? Parse(DatasetSpec spec, BqConnectionConfig cfg, List<string> errors)
    {
        var start = errors.Count;

        foreach (var key in spec.Options.Keys.Where(k => !KnownOptions.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown dataset option '{key}'; known options: {string.Join(", ", KnownOptions)}");
        }

        var entityText = GetString(spec.Options, "entity");
        var queryText = GetString(spec.Options, "query");
        var hasEntity = !string.IsNullOrEmpty(entityText);
        var hasQuery = !string.IsNullOrEmpty(queryText);

        if (hasEntity && hasQuery)
        {
            errors.Add("'entity' and 'query' cannot both be set on the same dataset; use one or the other");
        }

        TableRef? table = null;
        string? query = null;

        if (hasQuery)
        {
            query = queryText;
            if (cfg.StagingDataset is null)
            {
                errors.Add(BqCodes.Message(BqCodes.Config_QueryModeNeedsStagingDataset, cfg.Redactor,
                    "'query:' reads need 'staging_dataset' on the connection -- BigQuery stages a query's "
                    + "results into a temp table before the Storage Read API can stream them; set "
                    + "'staging_dataset' in connections.yml"));
            }
        }
        else
        {
            var entity = hasEntity ? entityText! : spec.Dataset;
            if (TableRef.TryParse(entity, cfg.Project, out var parsed, out var entityError))
            {
                table = parsed;
            }
            else
            {
                errors.Add(entityError!);
            }
        }

        var streams = ParseStreams(spec.Options, errors);

        return errors.Count == start ? new BqDatasetConfig(table, query, streams) : null;
    }

    private static int ParseStreams(IReadOnlyDictionary<string, object?> options, List<string> errors)
    {
        if (!options.TryGetValue("streams", out var raw) || raw is null)
        {
            return DefaultStreams;
        }

        long value;
        switch (raw)
        {
            case long l:
                value = l;
                break;
            case int i:
                value = i;
                break;
            default:
                errors.Add($"'streams' must be an integer between {MinStreams} and {MaxStreams} (got '{raw}')");
                return DefaultStreams;
        }

        if (value is < MinStreams or > MaxStreams)
        {
            errors.Add($"'streams' must be between {MinStreams} and {MaxStreams} (got {value})");
            return DefaultStreams;
        }

        return (int)value;
    }

    private static string? GetString(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var value) ? value?.ToString() : null;
}
