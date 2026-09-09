using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqOutputConfigTests
{
    private static BqConnectionConfig Connection() =>
        BqConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["project"] = "my-proj",
            ["auth"] = "adc",
        }), [])!;

    private static OutputSpec Spec(string output, Dictionary<string, object?>? options = null) =>
        new("bq", output, "append", "fail_on_change", options ?? new Dictionary<string, object?>());

    [Fact]
    public void Defaults_target_from_the_output_name()
    {
        var errors = new List<string>();
        var config = BqOutputConfig.Parse(Spec("sales.orders"), Connection(), errors);

        Assert.Empty(errors);
        Assert.NotNull(config);
        Assert.Equal(new TableRef("my-proj", "sales", "orders"), config!.Target);
    }

    [Fact]
    public void Entity_option_overrides_the_output_name()
    {
        var errors = new List<string>();
        var config = BqOutputConfig.Parse(Spec("ignored.ignored", new() { ["entity"] = "other.orders" }), Connection(), errors);

        Assert.Empty(errors);
        Assert.Equal(new TableRef("my-proj", "other", "orders"), config!.Target);
    }

    [Fact]
    public void Unknown_option_is_an_error_naming_the_key()
    {
        var errors = new List<string>();
        var config = BqOutputConfig.Parse(Spec("sales.orders", new() { ["bogus"] = "x" }), Connection(), errors);

        Assert.Null(config);
        Assert.Contains(errors, e => e.Contains("'bogus'"));
    }

    [Fact]
    public void Bad_entity_name_is_an_error()
    {
        var errors = new List<string>();
        var config = BqOutputConfig.Parse(Spec("not a valid entity name!!"), Connection(), errors);

        Assert.Null(config);
        Assert.NotEmpty(errors);
    }

    // BqOutputConfig.Parse itself never embeds a PZBQ code (unlike BqDatasetConfig.Parse's one
    // special-cased PZBQ0105) -- BqSink.BeginWriteAsync is the sole place these errors get wrapped
    // into PZBQ0307 (Write_BadOutputOption), covered by
    // BqWriteSessionTests.BeginWriteAsync_bad_output_option_is_PZBQ0307_with_no_network_call.
    [Fact]
    public void Parse_errors_carry_no_PZBQ_code_of_their_own()
    {
        var errors = new List<string>();
        BqOutputConfig.Parse(Spec("sales.orders", new() { ["bogus"] = "x" }), Connection(), errors);

        Assert.DoesNotContain(errors, e => e.Contains("PZBQ", StringComparison.Ordinal));
    }
}
