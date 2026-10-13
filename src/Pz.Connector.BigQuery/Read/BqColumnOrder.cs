using Apache.Arrow;

namespace Pz.Connector.BigQuery;

/// <summary>Column order for pruned reads. The Storage Read API returns the selected columns in table
/// order regardless of the order <c>selected_fields</c> lists them, while the host binds a batch's
/// columns by position in hint order.</summary>
internal static class BqColumnOrder
{
    /// <summary>The hinted names ordered by their position in the table schema; names the schema does
    /// not know keep their relative order at the end so the server reports them.</summary>
    public static IReadOnlyList<string> InTableOrder(IReadOnlyList<string> hinted, Schema schema) =>
        hinted
            .Select((name, i) => (name, i, pos: schema.FieldsList.ToList().FindIndex(f => f.Name == name)))
            .OrderBy(t => t.pos < 0 ? int.MaxValue : t.pos)
            .ThenBy(t => t.i)
            .Select(t => t.name)
            .ToList();

    /// <summary>Re-slots the batch's arrays into <paramref name="order"/> without copying. The result
    /// takes over the arrays; the original batch must not be disposed afterwards. A batch already in
    /// order, or lacking a hinted name, is returned unchanged.</summary>
    public static RecordBatch Reorder(RecordBatch batch, IReadOnlyList<string>? order)
    {
        if (order is not { Count: > 0 } || order.Count != batch.ColumnCount)
        {
            return batch;
        }

        var fields = new List<Field>(order.Count);
        var arrays = new List<IArrowArray>(order.Count);
        var inOrder = true;
        for (var i = 0; i < order.Count; i++)
        {
            var index = batch.Schema.GetFieldIndex(order[i]);
            if (index < 0)
            {
                return batch;
            }

            inOrder &= index == i;
            fields.Add(batch.Schema.GetFieldByIndex(index));
            arrays.Add(batch.Column(index));
        }

        return inOrder ? batch : new RecordBatch(new Schema(fields, batch.Schema.Metadata), arrays, batch.Length);
    }
}
