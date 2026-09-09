using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>BigQuery for pz: a source reads a table or a query result set through the Storage Read
/// API as Arrow; a sink is a load job landing rows by append, replace, or merge.</summary>
public sealed class BqConnector : IConnector, ISourceConnector, ISinkConnector
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly TimeProvider _time;

    public BqConnector(ILoggerFactory? loggerFactory = null)
        : this(loggerFactory, TimeProvider.System)
    {
    }

    internal BqConnector(ILoggerFactory? loggerFactory, TimeProvider time)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _time = time;
    }

    public ConnectorInfo Info { get; } = new(
        "bigquery",
        typeof(BqConnector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? "0.0.0",
        ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.BoundedWindow
        | ConnectorCapabilities.InclusiveWatermarkBound | ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.Merge
        | ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Transactional;

    public string ConnectionConfigSchema => """
        { "type": "object", "required": ["project", "auth"], "properties": {
            "project": { "type": "string" },
            "auth": { "type": "string", "enum": ["service_account", "adc", "none"] },
            "key_file": { "type": "string" },
            "key_json": { "type": "string" },
            "location": { "type": "string" },
            "staging_dataset": { "type": "string" },
            "endpoint": { "type": "string" },
            "storage_endpoint": { "type": "string" },
            "base_dir": { "type": "string" } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => """
        { "type": "object", "properties": {
            "entity": { "type": "string" },
            "query": { "type": "string" },
            "streams": { "type": "integer", "minimum": 1, "maximum": 64 } },
          "additionalProperties": false }
        """;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        BqConnectionConfig.Parse(config, errors);
        return ValueTask.FromResult(errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]));
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();
}
