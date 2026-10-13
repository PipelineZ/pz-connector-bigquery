using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqColumnOrderTests
{
    private static RecordBatch TableOrderBatch() => new RecordBatch.Builder()
        .Append("id", false, c => c.Int64(a => a.AppendRange([1L, 2L, 3L])))
        .Append("name", false, c => c.String(a => a.AppendRange(["ann", "bob", "cy"])))
        .Build();

    [Fact]
    public void Reversed_hint_yields_columns_in_hint_order_with_values_intact()
    {
        var result = BqColumnOrder.Reorder(TableOrderBatch(), ["name", "id"]);

        Assert.Equal(["name", "id"], result.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(3, result.Length);
        Assert.Equal("bob", ((StringArray)result.Column(0)).GetString(1));
        Assert.Equal(3L, ((Int64Array)result.Column(1)).GetValue(2));
    }

    [Fact]
    public void Batch_already_in_order_or_missing_a_name_is_returned_as_is()
    {
        var batch = TableOrderBatch();

        Assert.Same(batch, BqColumnOrder.Reorder(batch, ["id", "name"]));
        Assert.Same(batch, BqColumnOrder.Reorder(batch, ["name", "nope"]));
        Assert.Same(batch, BqColumnOrder.Reorder(batch, null));
    }

    [Fact]
    public void Selected_fields_are_requested_in_table_order()
    {
        var schema = new Schema.Builder().Field(new Field("id", Int64Type.Default, false)).Field(new Field("name", StringType.Default, false)).Build();

        Assert.Equal(["id", "name"], BqColumnOrder.InTableOrder(["name", "id"], schema));
        Assert.Equal(["id", "zzz"], BqColumnOrder.InTableOrder(["zzz", "id"], schema));
    }
}
