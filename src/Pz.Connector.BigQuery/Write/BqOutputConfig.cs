using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>An output's write options, parsed once at <c>BeginWriteAsync</c>. Unlike
/// <see cref="BqDatasetConfig"/> there is no <c>query:</c> counterpart -- a write always names a
/// concrete table.</summary>
internal sealed record BqOutputConfig(TableRef Target)
{
    private static readonly string[] KnownOptions = ["entity"];

    public static BqOutputConfig? Parse(OutputSpec spec, BqConnectionConfig cfg, List<string> errors)
    {
        var start = errors.Count;

        foreach (var key in spec.Options.Keys.Where(k => !KnownOptions.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown output option '{key}'; known options: {string.Join(", ", KnownOptions)}");
        }

        var entityText = GetString(spec.Options, "entity");
        var entity = string.IsNullOrEmpty(entityText) ? spec.Output : entityText;

        TableRef? target = null;
        if (TableRef.TryParse(entity, cfg.Project, out var parsed, out var entityError))
        {
            target = parsed;
        }
        else
        {
            errors.Add(entityError!);
        }

        return errors.Count == start && target is { } t ? new BqOutputConfig(t) : null;
    }

    private static string? GetString(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var value) ? value?.ToString() : null;
}
