using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqDatasetConfigTests
{
    private static BqConnectionConfig Connection(TableRef? stagingDataset = null) =>
        BqConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["project"] = "my-proj",
            ["auth"] = "adc",
            ["staging_dataset"] = stagingDataset is null ? null : $"{stagingDataset.Value.Project}.{stagingDataset.Value.Dataset}",
        }), [])!;

    private static DatasetSpec Spec(string dataset, Dictionary<string, object?>? options = null) =>
        new("bq", dataset, options ?? new Dictionary<string, object?>());

    [Fact]
    public void Defaults_entity_from_the_dataset_name_and_four_streams()
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(Spec("sales.orders"), Connection(), errors);

        Assert.Empty(errors);
        Assert.NotNull(config);
        Assert.Equal(new TableRef("my-proj", "sales", "orders"), config!.Table);
        Assert.Null(config.Query);
        Assert.Equal(4, config.Streams);
    }

    [Fact]
    public void Entity_option_overrides_the_dataset_name()
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(Spec("ignored.ignored", new() { ["entity"] = "other.orders" }), Connection(), errors);

        Assert.Empty(errors);
        Assert.Equal(new TableRef("my-proj", "other", "orders"), config!.Table);
    }

    [Fact]
    public void Query_without_staging_dataset_is_PZBQ0105()
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(Spec("sales.orders", new() { ["query"] = "select 1" }), Connection(), errors);

        Assert.Null(config);
        Assert.Contains(errors, e => e.Contains("PZBQ0105"));
    }

    [Fact]
    public void Query_with_staging_dataset_sets_query_and_no_table()
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(
            Spec("sales.orders", new() { ["query"] = "select 1" }),
            Connection(new TableRef("my-proj", "stage", "")),
            errors);

        Assert.Empty(errors);
        Assert.NotNull(config);
        Assert.Null(config!.Table);
        Assert.Equal("select 1", config.Query);
    }

    [Fact]
    public void Entity_and_query_together_is_an_error()
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(
            Spec("sales.orders", new() { ["entity"] = "x.y", ["query"] = "select 1" }),
            Connection(new TableRef("my-proj", "stage", "")),
            errors);

        Assert.Null(config);
        Assert.Contains(errors, e => e.Contains("'entity'") && e.Contains("'query'"));
    }

    [Theory]
    [InlineData(1L)]
    [InlineData(64L)]
    [InlineData(8L)]
    [InlineData(1)]
    [InlineData(64)]
    // A whole-number double is the only numeric shape an out-of-process caller can send: PCP
    // carries dataset options through a protobuf Struct, whose one numeric kind is `double`
    // (google.protobuf.Value.NumberValue) -- every real run hits this branch, not long/int.
    [InlineData(1.0)]
    [InlineData(64.0)]
    [InlineData(8.0)]
    public void Streams_accepts_long_int_or_whole_double_in_range(object streams)
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(Spec("sales.orders", new() { ["streams"] = streams }), Connection(), errors);

        Assert.Empty(errors);
        Assert.Equal(Convert.ToInt32(streams), config!.Streams);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(65L)]
    [InlineData(0)]
    [InlineData(65)]
    [InlineData(0.0)]
    [InlineData(65.0)]
    public void Streams_out_of_range_is_refused(object streams)
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(Spec("sales.orders", new() { ["streams"] = streams }), Connection(), errors);

        Assert.Null(config);
        Assert.Contains(errors, e => e.Contains("'streams'"));
    }

    [Theory]
    [InlineData("4")]
    // A fractional double is not a whole stream count even though it is the wire's numeric kind.
    [InlineData(1.5)]
    public void Streams_of_the_wrong_type_is_refused(object streams)
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(Spec("sales.orders", new() { ["streams"] = streams }), Connection(), errors);

        Assert.Null(config);
        Assert.Contains(errors, e => e.Contains("'streams'"));
    }

    [Fact]
    public void Unknown_option_is_refused_naming_the_known_ones()
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(Spec("sales.orders", new() { ["bogus"] = "x" }), Connection(), errors);

        Assert.Null(config);
        Assert.Contains(errors, e => e.Contains("'bogus'") && e.Contains("entity") && e.Contains("query") && e.Contains("streams"));
    }

    [Fact]
    public void Invalid_entity_name_surfaces_TableRefs_own_error()
    {
        var errors = new List<string>();
        var config = BqDatasetConfig.Parse(Spec("just_one_part"), Connection(), errors);

        Assert.Null(config);
        Assert.Contains(errors, e => e.Contains("dataset.table"));
    }
}
