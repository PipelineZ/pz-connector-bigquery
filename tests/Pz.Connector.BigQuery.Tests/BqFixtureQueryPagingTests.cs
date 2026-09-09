using System.Text.Json;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>Pure unit coverage for <see cref="BqFixture.AssembleRows"/> -- no docker/emulator
/// needed. Exists because the emulator itself ignores <c>maxResults</c> and never actually pages a
/// query result (verified against the live container: a 3-row result with <c>maxResults: 1</c> still
/// came back as one page with no <c>pageToken</c>), so the multi-page assembly path cannot be
/// exercised end to end and gets deterministic coverage here instead.</summary>
public sealed class BqFixtureQueryPagingTests
{
    private static JsonElement Page(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public void AssembleRows_combines_rows_from_every_page_using_the_first_pages_schema()
    {
        var first = Page("""
            {"schema":{"fields":[{"name":"id","type":"INTEGER"}]},"rows":[{"f":[{"v":"1"}]}],"pageToken":"t1","jobComplete":true}
            """);
        var second = Page("""{"rows":[{"f":[{"v":"2"}]}],"pageToken":"t2"}""");
        var third = Page("""{"rows":[{"f":[{"v":"3"}]}]}""");

        var rows = BqFixture.AssembleRows([first, second, third]);

        Assert.Equal(3, rows.Count);
        Assert.Equal("1", rows[0].GetProperty("id").GetString());
        Assert.Equal("2", rows[1].GetProperty("id").GetString());
        Assert.Equal("3", rows[2].GetProperty("id").GetString());
    }

    [Fact]
    public void AssembleRows_ignores_pages_with_no_rows_property()
    {
        var withSchema = Page("""{"schema":{"fields":[{"name":"n","type":"INTEGER"}]},"jobComplete":true}""");

        var rows = BqFixture.AssembleRows([withSchema]);

        Assert.Empty(rows);
    }

    [Fact]
    public void AssembleRows_maps_null_cells_to_json_null()
    {
        var page = Page("""
            {"schema":{"fields":[{"name":"n","type":"STRING"}]},"rows":[{"f":[{"v":null}]}]}
            """);

        var rows = BqFixture.AssembleRows([page]);

        Assert.Equal(JsonValueKind.Null, rows[0].GetProperty("n").ValueKind);
    }
}
