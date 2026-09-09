namespace Pz.Connector.BigQuery.Tests;

public sealed class BqTargetSchemaTests
{
    private static BqFieldSchema Field(string name, string type, string mode = "NULLABLE", string? precision = null, string? scale = null) =>
        new(name, type, mode, precision, scale, null);

    private static BqTableSchema Schema(params BqFieldSchema[] fields) => new(fields);

    [Fact]
    public void Identical_schemas_have_no_mismatches()
    {
        var existing = Schema(Field("id", "INTEGER"), Field("name", "STRING"));
        var wanted = Schema(Field("id", "INTEGER"), Field("name", "STRING"));

        Assert.Empty(BqTargetSchema.Diff(existing, wanted));
    }

    [Fact]
    public void Missing_column_is_a_mismatch()
    {
        var existing = Schema(Field("id", "INTEGER"));
        var wanted = Schema(Field("id", "INTEGER"), Field("name", "STRING"));

        var diff = BqTargetSchema.Diff(existing, wanted);

        Assert.Single(diff);
        Assert.Contains("name", diff[0]);
    }

    [Fact]
    public void Type_mismatch_names_both_types()
    {
        var existing = Schema(Field("id", "STRING"));
        var wanted = Schema(Field("id", "INTEGER"));

        var diff = BqTargetSchema.Diff(existing, wanted);

        Assert.Single(diff);
        Assert.Contains("STRING", diff[0]);
        Assert.Contains("INTEGER", diff[0]);
    }

    [Fact]
    public void Repeated_mode_is_a_mismatch()
    {
        var existing = Schema(Field("tags", "STRING", mode: "REPEATED"));
        var wanted = Schema(Field("tags", "STRING"));

        var diff = BqTargetSchema.Diff(existing, wanted);

        Assert.Single(diff);
        Assert.Contains("tags", diff[0]);
    }

    [Fact]
    public void Required_target_column_is_compatible()
    {
        var existing = Schema(Field("id", "INTEGER", mode: "REQUIRED"));
        var wanted = Schema(Field("id", "INTEGER"));

        Assert.Empty(BqTargetSchema.Diff(existing, wanted));
    }

    [Fact]
    public void Extra_target_columns_are_allowed()
    {
        var existing = Schema(Field("id", "INTEGER"), Field("created_at", "TIMESTAMP"));
        var wanted = Schema(Field("id", "INTEGER"));

        Assert.Empty(BqTargetSchema.Diff(existing, wanted));
    }

    [Fact]
    public void Legacy_integer_from_tables_get_equals_int64_from_the_write_schema()
    {
        var existing = Schema(Field("id", "INTEGER"));
        var wanted = Schema(Field("id", "INT64"));

        Assert.Empty(BqTargetSchema.Diff(existing, wanted));
    }

    [Fact]
    public void Legacy_float_and_boolean_normalize_the_same_way()
    {
        var existing = Schema(Field("f", "FLOAT"), Field("b", "BOOLEAN"));
        var wanted = Schema(Field("f", "FLOAT64"), Field("b", "BOOL"));

        Assert.Empty(BqTargetSchema.Diff(existing, wanted));
    }

    [Fact]
    public void Numeric_precision_mismatch_is_flagged_when_target_declares_precision()
    {
        var existing = Schema(Field("amount", "NUMERIC", precision: "38", scale: "9"));
        var wanted = Schema(Field("amount", "NUMERIC", precision: "10", scale: "2"));

        var diff = BqTargetSchema.Diff(existing, wanted);

        Assert.Single(diff);
        Assert.Contains("amount", diff[0]);
    }

    [Fact]
    public void Numeric_precision_is_not_checked_when_target_declares_none()
    {
        var existing = Schema(Field("amount", "NUMERIC"));
        var wanted = Schema(Field("amount", "NUMERIC", precision: "10", scale: "2"));

        Assert.Empty(BqTargetSchema.Diff(existing, wanted));
    }

    [Fact]
    public void Numeric_precision_match_is_not_flagged()
    {
        var existing = Schema(Field("amount", "NUMERIC", precision: "38", scale: "9"));
        var wanted = Schema(Field("amount", "NUMERIC", precision: "38", scale: "9"));

        Assert.Empty(BqTargetSchema.Diff(existing, wanted));
    }

    [Fact]
    public void Every_wanted_column_is_checked_independently()
    {
        var existing = Schema(Field("a", "STRING"));
        var wanted = Schema(Field("a", "INTEGER"), Field("b", "STRING"));

        var diff = BqTargetSchema.Diff(existing, wanted);

        Assert.Equal(2, diff.Count);
    }
}
