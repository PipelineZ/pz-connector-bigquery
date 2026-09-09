using System.Net.Http;
using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqConnectorTests
{
    private static ConnectorConfig Config(Dictionary<string, object?> values) => new(values);

    // "none" auth + a fake endpoint, matching BqRestClientTests' own Config() shape: no live
    // credential is ever needed to exercise CheckConnectionAsync's REST-call path through a
    // FakeHandler.
    private static ConnectorConfig FakeConfig(string endpoint = "http://fake/") => Config(new()
    {
        ["project"] = "test", ["auth"] = "none", ["endpoint"] = endpoint,
    });

    private static BqConnector ConnectorOver(HttpMessageHandler handler) =>
        new(null, TimeProvider.System, 64 * 1024 * 1024, () => new HttpClient(handler));

    [Fact]
    public async Task CheckConnectionAsync_reports_the_visible_dataset_count()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/test/datasets?maxResults=1000", 200,
            """{"datasets":[{"id":"test:a"},{"id":"test:b"}]}""");
        var connector = ConnectorOver(handler);

        var check = await connector.CheckConnectionAsync(FakeConfig(), CancellationToken.None);

        Assert.True(check.Ok);
        Assert.Equal("project test: 2 dataset(s) visible", check.Message);
    }

    [Fact]
    public async Task CheckConnectionAsync_reports_authenticated_when_no_dataset_is_visible()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/test/datasets?maxResults=1000", 200, """{"datasets":[]}""");
        var connector = ConnectorOver(handler);

        var check = await connector.CheckConnectionAsync(FakeConfig(), CancellationToken.None);

        Assert.True(check.Ok);
        Assert.Equal("project test: authenticated", check.Message);
    }

    [Fact]
    public async Task CheckConnectionAsync_never_throws_on_a_permission_denied_response()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/test/datasets?maxResults=1000", 403,
            """{"error":{"code":403,"message":"denied","errors":[{"reason":"accessDenied"}]}}""");
        var connector = ConnectorOver(handler);

        var check = await connector.CheckConnectionAsync(FakeConfig(), CancellationToken.None);

        Assert.False(check.Ok);
        Assert.NotNull(check.Message);
        Assert.Contains("PZBQ0405", check.Message);
    }

    [Fact]
    public async Task CheckConnectionAsync_reports_parse_errors_without_a_network_call()
    {
        var handler = new FakeHandler();
        var connector = ConnectorOver(handler);
        var config = Config(new() { ["auth"] = "bogus" });
        var expected = new List<string>();
        BqConnectionConfig.Parse(config, expected);

        var check = await connector.CheckConnectionAsync(config, CancellationToken.None);

        Assert.False(check.Ok);
        Assert.Equal(string.Join("; ", expected), check.Message);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task CheckConnectionAsync_propagates_cancellation_unwrapped()
    {
        using var cts = new CancellationTokenSource();
        var connector = ConnectorOver(new ThrowingHandler(new OperationCanceledException(cts.Token)));
        await cts.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => connector.CheckConnectionAsync(FakeConfig(), cts.Token).AsTask());
    }

    /// <summary>Throws <paramref name="exception"/> instead of producing a response -- lets a test
    /// hand <see cref="BqConnector.CheckConnectionAsync"/> a caller-cancellation failure it must
    /// propagate unwrapped rather than swallow into <c>ConnectionCheck.Ok = false</c>.</summary>
    private sealed class ThrowingHandler(Exception exception) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) =>
            throw exception;
    }

    [Fact]
    public async Task CheckConnectionAsync_disposes_its_own_HttpClient_before_returning()
    {
        var handler = new FakeHandler();
        handler.Add(HttpMethod.Get, "/bigquery/v2/projects/test/datasets?maxResults=1000", 200, """{"datasets":[]}""");
        var connector = ConnectorOver(handler);

        await connector.CheckConnectionAsync(FakeConfig(), CancellationToken.None);

        Assert.True(handler.Disposed);
    }

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
