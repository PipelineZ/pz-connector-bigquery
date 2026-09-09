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
    private readonly long _spoolRollBytes;

    public BqConnector(ILoggerFactory? loggerFactory = null)
        : this(loggerFactory, TimeProvider.System)
    {
    }

    internal BqConnector(ILoggerFactory? loggerFactory, TimeProvider time, long spoolRollBytes = 64 * 1024 * 1024)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _time = time;
        _spoolRollBytes = spoolRollBytes;
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

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        var credential = BqAuth.Create(connection);
        var rest = new BqRestClient(new HttpClient(), connection, credential, connection.Redactor,
            _loggerFactory.CreateLogger<BqRestClient>());
        var factory = new BqReadSessionFactory(connection, credential, connection.Redactor);
        var materializer = new BqQueryMaterializer(rest, connection, _time, _loggerFactory.CreateLogger<BqQueryMaterializer>());
        return ValueTask.FromResult<ISource>(
            new BqSource(connection, rest, factory, connection.Redactor, _loggerFactory.CreateLogger<BqSource>(), _time, materializer));
    }

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        var credential = BqAuth.Create(connection);
        var rest = new BqRestClient(new HttpClient(), connection, credential, connection.Redactor,
            _loggerFactory.CreateLogger<BqRestClient>());
        return ValueTask.FromResult<ISink>(
            new BqSink(connection, rest, connection.Redactor, _loggerFactory.CreateLogger<BqSink>(), _time, _spoolRollBytes));
    }

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();

    // Redaction-free: BqConnectionConfig.Parse never embeds a secret's own text in an error message
    // (every error names a key or shape, never the value that failed), so no BqRedactor built from a
    // successful parse exists yet to route this failure message through.
    private static BqConnectionConfig ParseOrThrow(ConnectorConfig config)
    {
        var errors = new List<string>();
        return BqConnectionConfig.Parse(config, errors)
            ?? throw new PzConnectorException(
                BqCodes.Message(BqCodes.Config_Invalid, BqRedactor.None, string.Join("; ", errors)), isTransient: false);
    }
}
