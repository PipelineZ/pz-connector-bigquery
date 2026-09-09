using System.Text;

namespace Pz.Connector.BigQuery;

/// <summary>Builds the GoogleSQL text for the one job that touches the write target
/// (<c>CommitAsync</c> step 4): append's DML, the query text a query-destination job runs for
/// append/replace, and the full merge statement. Every identifier is backtick-quoted through
/// <see cref="TableRef.QuoteIdentifier"/> so the generated text is never reparsed against a caller's
/// own quoting assumptions.</summary>
internal static class BqSql
{
    /// <summary>DML that respects the existing target schema and any of its extra columns -- a
    /// query-destination append would instead demand an exact schema match.</summary>
    public static string Append(TableRef target, TableRef staging, IReadOnlyList<string> cols) =>
        $"insert into {target.Quoted} ({ColumnList(cols)}) {Select(staging, cols)}";

    /// <summary>The query a query-destination job (append when the target is missing, or replace)
    /// runs against the staging table.</summary>
    public static string Select(TableRef staging, IReadOnlyList<string> cols) =>
        $"select {ColumnList(cols)} from {staging.Quoted}";

    /// <summary>The deduplicated final state a merge would insert against an empty target: last
    /// <c>_pz_seq</c> within the session wins per key, exactly like <see cref="Merge"/>'s own
    /// <c>using (...)</c> subquery. Run as a query-destination job's SQL to create a merge target
    /// that does not exist yet -- <c>merge</c>'s own DML syntax requires an existing target, so
    /// creating one from nothing is a plain select into a new destination, never a MERGE
    /// statement.</summary>
    public static string DedupedSelect(TableRef staging, IReadOnlyList<string> cols, IReadOnlyList<string> keys)
    {
        var colList = ColumnList(cols);
        var keyList = ColumnList(keys);

        var sql = new StringBuilder();
        sql.Append("select ").Append(colList).Append(" from (\n");
        sql.Append("  select ").Append(colList)
            .Append(", row_number() over (partition by ").Append(keyList).Append(" order by `_pz_seq` desc) as `_pz_rn`\n");
        sql.Append("  from ").Append(staging.Quoted).Append('\n');
        sql.Append(") where `_pz_rn` = 1");
        return sql.ToString();
    }

    /// <summary>Deduplicates staged rows on the merge keys (last <c>_pz_seq</c> within the session
    /// wins), then upserts against the target. The <c>on</c> clause treats null keys as matching
    /// null keys, since GoogleSQL's own <c>=</c> would otherwise never match two nulls. <c>when
    /// matched</c> is omitted entirely when every column is a key -- there is then nothing left to
    /// update, since the key columns already proved equal in <c>on</c>.</summary>
    public static string Merge(TableRef target, TableRef staging, IReadOnlyList<string> cols, IReadOnlyList<string> keys)
    {
        var colList = ColumnList(cols);
        var keyList = ColumnList(keys);
        var on = string.Join(" and ", keys.Select(k => $"(t.{Q(k)} = s.{Q(k)} or (t.{Q(k)} is null and s.{Q(k)} is null))"));
        var nonKeyCols = cols.Where(c => !keys.Contains(c, StringComparer.Ordinal)).ToList();
        var insertValues = string.Join(", ", cols.Select(c => $"s.{Q(c)}"));

        var sql = new StringBuilder();
        sql.Append("merge ").Append(target.Quoted).Append(" t\n");
        sql.Append("using (\n");
        sql.Append("  select ").Append(colList).Append(" from (\n");
        sql.Append("    select ").Append(colList)
            .Append(", row_number() over (partition by ").Append(keyList).Append(" order by `_pz_seq` desc) as `_pz_rn`\n");
        sql.Append("    from ").Append(staging.Quoted).Append('\n');
        sql.Append("  ) where `_pz_rn` = 1\n");
        sql.Append(") s\n");
        sql.Append("on ").Append(on).Append('\n');

        if (nonKeyCols.Count > 0)
        {
            var updateSet = string.Join(", ", nonKeyCols.Select(c => $"t.{Q(c)} = s.{Q(c)}"));
            sql.Append("when matched then update set ").Append(updateSet).Append('\n');
        }

        sql.Append("when not matched then insert (").Append(colList).Append(") values (").Append(insertValues).Append(')');
        return sql.ToString();
    }

    private static string ColumnList(IReadOnlyList<string> cols) => string.Join(", ", cols.Select(Q));

    private static string Q(string column) => TableRef.QuoteIdentifier(column);
}
