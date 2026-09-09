using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqConnectorTests
{
    private static ConnectorConfig Config(Dictionary<string, object?> values) => new(values);

    [Fact]
    public async Task ValidateAsync_is_valid_for_a_well_formed_connection()
    {
        var connector = new BqConnector();

        var result = await connector.ValidateAsync(Config(new()
        {
            ["project"] = "my-proj", ["auth"] = "adc",
        }), CancellationToken.None);

        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public async Task ValidateAsync_surfaces_the_same_messages_as_BqConnectionConfig_Parse()
    {
        var connector = new BqConnector();
        var config = Config(new() { ["auth"] = "bogus" });

        var result = await connector.ValidateAsync(config, CancellationToken.None);

        var expected = new List<string>();
        BqConnectionConfig.Parse(config, expected);

        Assert.False(result.IsValid);
        Assert.Equal(expected, result.Errors);
    }

    [Fact]
    public void Info_and_capabilities()
    {
        var connector = new BqConnector();

        Assert.Equal("bigquery", connector.Info.Name);
        Assert.Equal(ProtocolVersion.Major, connector.Info.ProtocolMajor);
        Assert.Equal(
            ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.BoundedWindow
            | ConnectorCapabilities.InclusiveWatermarkBound | ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.Merge
            | ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Transactional,
            connector.Capabilities);

        using var connection = JsonDocument.Parse(connector.ConnectionConfigSchema);
        Assert.False(connection.RootElement.GetProperty("additionalProperties").GetBoolean());

        using var dataset = JsonDocument.Parse(connector.DatasetConfigSchema);
        Assert.False(dataset.RootElement.GetProperty("additionalProperties").GetBoolean());
    }
}
