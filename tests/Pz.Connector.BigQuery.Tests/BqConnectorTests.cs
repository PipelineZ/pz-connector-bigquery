using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqConnectorTests
{
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
