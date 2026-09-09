using System.Reflection;
using Google.Apis.Auth.OAuth2;
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
    private readonly Func<HttpClient> _httpClientFactory;

    public BqConnector(ILoggerFactory? loggerFactory = null)
        : this(loggerFactory, TimeProvider.System)
    {
    }

    internal BqConnector(ILoggerFactory? loggerFactory, TimeProvider time, long spoolRollBytes = 64 * 1024 * 1024)
        : this(loggerFactory, time, spoolRollBytes, static () => new HttpClient())
    {
    }

    /// <summary>Test-only: supplies the <see cref="HttpClient"/> every REST call -- source open, sink
    /// open, and <see cref="CheckConnectionAsync"/> alike -- goes through, so a <c>FakeHandler</c> can
    /// stand in for the network. Every production constructor above funnels here with a plain
    /// <c>new HttpClient()</c> factory.</summary>
    internal BqConnector(ILoggerFactory? loggerFactory, TimeProvider time, long spoolRollBytes, Func<HttpClient> httpClientFactory)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _time = time;
        _spoolRollBytes = spoolRollBytes;
        _httpClientFactory = httpClientFactory;
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
        var (credential, rest) = OpenRest(connection);
        var factory = new BqReadSessionFactory(connection, credential, connection.Redactor);
        var materializer = new BqQueryMaterializer(rest, connection, _time, _loggerFactory.CreateLogger<BqQueryMaterializer>());
        return ValueTask.FromResult<ISource>(
            new BqSource(connection, rest, factory, connection.Redactor, _loggerFactory.CreateLogger<BqSource>(), _time, materializer));
    }

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        var (_, rest) = OpenRest(connection);
        return ValueTask.FromResult<ISink>(
            new BqSink(connection, rest, connection.Redactor, _loggerFactory.CreateLogger<BqSink>(), _time, _spoolRollBytes));
    }

    /// <summary>Counts visible datasets as the cheapest call that proves both the credential and the
    /// project are usable. Never throws for a remote or auth failure -- <see cref="ParseOrThrow"/>'s
    /// PZBQ0109-wrapped message would misdescribe a config problem here as a connector exception, so
    /// parse errors are reported directly instead; a <see cref="PzConnectorException"/> from
    /// <see cref="OpenRest"/> (a bad credential) or the REST call itself already carries its own PZBQ
    /// code and is reported unwrapped. Caller cancellation is not caught, so it propagates unwrapped
    /// like every other REST path in this connector.</summary>
    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = BqConnectionConfig.Parse(config, errors);
        if (connection is null)
        {
            return new ConnectionCheck(false, string.Join("; ", errors));
        }

        try
        {
            var (_, rest) = OpenRest(connection);
            var count = await rest.CountDatasetsAsync(connection.Project, ct).ConfigureAwait(false);
            return new ConnectionCheck(true, count > 0
                ? $"project {connection.Project}: {count} dataset(s) visible"
                : $"project {connection.Project}: authenticated");
        }
        catch (PzConnectorException ex)
        {
            return new ConnectionCheck(false, ex.Message);
        }
    }

    /// <summary>The one place that turns a parsed connection into the credential and REST client
    /// every open (source, sink, connection check) sends its calls through.</summary>
    private (GoogleCredential? Credential, BqRestClient RestClient) OpenRest(BqConnectionConfig connection)
    {
        var credential = BqAuth.Create(connection);
        var rest = new BqRestClient(_httpClientFactory(), connection, credential, connection.Redactor,
            _loggerFactory.CreateLogger<BqRestClient>());
        return (credential, rest);
    }

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
