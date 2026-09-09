namespace Pz.Connector.BigQuery.Tests;

public sealed class BqSqlTests
{
    private static readonly TableRef Target = new("p", "d", "target");
    private static readonly TableRef Staging = new("p", "d", "staging");

    [Fact]
    public void Append_is_an_insert_select_from_staging()
    {
        var sql = BqSql.Append(Target, Staging, ["id", "name", "amount"]);

        Assert.Equal(
            "insert into `p`.`d`.`target` (`id`, `name`, `amount`) "
            + "select `id`, `name`, `amount` from `p`.`d`.`staging`",
            sql);
    }

    [Fact]
    public void Select_lists_columns_from_staging()
    {
        var sql = BqSql.Select(Staging, ["id", "name", "amount"]);

        Assert.Equal("select `id`, `name`, `amount` from `p`.`d`.`staging`", sql);
    }

    [Fact]
    public void Merge_with_three_columns_one_key()
    {
        var sql = BqSql.Merge(Target, Staging, ["id", "name", "amount"], ["id"]);

        var expected =
            "merge `p`.`d`.`target` t\n"
            + "using (\n"
            + "  select `id`, `name`, `amount` from (\n"
            + "    select `id`, `name`, `amount`, row_number() over (partition by `id` order by `_pz_seq` desc) as `_pz_rn`\n"
            + "    from `p`.`d`.`staging`\n"
            + "  ) where `_pz_rn` = 1\n"
            + ") s\n"
            + "on (t.`id` = s.`id` or (t.`id` is null and s.`id` is null))\n"
            + "when matched then update set t.`name` = s.`name`, t.`amount` = s.`amount`\n"
            + "when not matched then insert (`id`, `name`, `amount`) values (s.`id`, s.`name`, s.`amount`)";

        Assert.Equal(expected, sql);
    }

    [Fact]
    public void Merge_where_every_column_is_a_key_omits_when_matched()
    {
        var sql = BqSql.Merge(Target, Staging, ["id"], ["id"]);

        var expected =
            "merge `p`.`d`.`target` t\n"
            + "using (\n"
            + "  select `id` from (\n"
            + "    select `id`, row_number() over (partition by `id` order by `_pz_seq` desc) as `_pz_rn`\n"
            + "    from `p`.`d`.`staging`\n"
            + "  ) where `_pz_rn` = 1\n"
            + ") s\n"
            + "on (t.`id` = s.`id` or (t.`id` is null and s.`id` is null))\n"
            + "when not matched then insert (`id`) values (s.`id`)";

        Assert.Equal(expected, sql);
    }

    [Fact]
    public void Merge_with_two_keys_joins_on_clauses_with_and()
    {
        var sql = BqSql.Merge(Target, Staging, ["k1", "k2", "v"], ["k1", "k2"]);

        Assert.Contains(
            "on (t.`k1` = s.`k1` or (t.`k1` is null and s.`k1` is null)) "
            + "and (t.`k2` = s.`k2` or (t.`k2` is null and s.`k2` is null))",
            sql);
        Assert.Contains("partition by `k1`, `k2`", sql);
    }

    [Fact]
    public void Identifiers_with_special_characters_are_backtick_escaped()
    {
        var target = new TableRef("p", "d", "ta`ble");
        var sql = BqSql.Select(target, ["co\\l"]);

        Assert.Equal("select `co\\\\l` from `p`.`d`.`ta\\`ble`", sql);
    }
}
