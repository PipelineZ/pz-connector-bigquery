namespace Pz.Connector.BigQuery.Tests;

[Collection("bigquery")]
public sealed class BqFixtureTests
{
    private readonly BqFixture _fixture;

    public BqFixtureTests(BqFixture fixture)
    {
        DockerFacts.SkipUnlessDocker();
        _fixture = fixture;
    }

    [SkippableFact]
    public async Task Fixture_starts_and_answers()
    {
        var table = BqFixture.NewName("smoke");
        await _fixture.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]""");

        await _fixture.InsertRowsAsync(table, new[]
        {
            """{"id":1,"name":"a"}""",
            """{"id":2,"name":"b"}""",
            """{"id":3,"name":"c"}""",
        });

        var rows = await _fixture.QueryAsync($"select count(*) as n from `{BqFixture.Project}.{BqFixture.Dataset}.{table}`");

        Assert.Single(rows);
        Assert.Equal("3", rows[0].GetProperty("n").GetString());
    }
}
