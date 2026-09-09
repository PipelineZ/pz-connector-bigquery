namespace Pz.Connector.BigQuery.Tests;

public sealed class TableRefTests
{
    [Fact]
    public void Two_part_name_uses_the_default_project()
    {
        Assert.True(TableRef.TryParse("sales.orders", "my-proj", out var result, out var error));
        Assert.Null(error);
        Assert.Equal(new TableRef("my-proj", "sales", "orders"), result);
    }

    [Fact]
    public void Three_part_name_carries_its_own_project()
    {
        Assert.True(TableRef.TryParse("p.sales.orders", "my-proj", out var result, out var error));
        Assert.Null(error);
        Assert.Equal(new TableRef("p", "sales", "orders"), result);
    }

    [Fact]
    public void One_part_name_is_refused_naming_both_accepted_shapes()
    {
        Assert.False(TableRef.TryParse("orders", "my-proj", out _, out var error));
        Assert.Contains("dataset.table", error);
        Assert.Contains("project.dataset.table", error);
    }

    [Fact]
    public void Four_part_name_is_refused()
    {
        Assert.False(TableRef.TryParse("a.b.c.d", "my-proj", out _, out var error));
        Assert.NotNull(error);
    }

    [Fact]
    public void Backtick_anywhere_is_refused_with_the_backtick_error()
    {
        Assert.False(TableRef.TryParse("`sales`.`orders`", "my-proj", out _, out var error));
        Assert.Contains("backticks are not accepted", error);
    }

    [Fact]
    public void Table_id_allows_hyphens()
    {
        Assert.True(TableRef.TryParse("sales.order-2026", "my-proj", out var result, out var error));
        Assert.Null(error);
        Assert.Equal("order-2026", result.Table);
    }

    [Fact]
    public void Table_id_with_a_space_is_refused()
    {
        Assert.False(TableRef.TryParse("sales.bad name", "my-proj", out _, out var error));
        Assert.NotNull(error);
    }

    [Fact]
    public void Quoted_escapes_a_backtick_inside_a_part()
    {
        var result = new TableRef("p", "d", "weird`table");
        Assert.Equal("`p`.`d`.`weird\\`table`", result.Quoted);
    }

    [Fact]
    public void Quoted_escapes_a_backslash_inside_a_part()
    {
        var result = new TableRef("p", "d", @"weird\table");
        Assert.Equal(@"`p`.`d`.`weird\\table`", result.Quoted);
    }

    [Fact]
    public void Resource_path_is_the_rest_shape()
    {
        var result = new TableRef("p", "d", "t");
        Assert.Equal("projects/p/datasets/d/tables/t", result.ResourcePath);
    }

    [Fact]
    public void Dataset_only_ref_accepts_bare_and_qualified_forms()
    {
        Assert.True(TableRef.TryParseDataset("pz_staging", "my-proj", out var bare, out var error1));
        Assert.Null(error1);
        Assert.Equal(new TableRef("my-proj", "pz_staging", ""), bare);

        Assert.True(TableRef.TryParseDataset("other.pz_staging", "my-proj", out var qualified, out var error2));
        Assert.Null(error2);
        Assert.Equal(new TableRef("other", "pz_staging", ""), qualified);
    }

    [Fact]
    public void Dataset_only_ref_rejects_backticks_and_bad_shapes()
    {
        Assert.False(TableRef.TryParseDataset("`d`", "my-proj", out _, out var error));
        Assert.Contains("backticks are not accepted", error);

        Assert.False(TableRef.TryParseDataset("a.b.c", "my-proj", out _, out var shapeError));
        Assert.NotNull(shapeError);
    }
}
