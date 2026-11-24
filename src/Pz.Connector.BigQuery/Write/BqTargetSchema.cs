namespace Pz.Connector.BigQuery;

/// <summary>Compares a write's mapped schema against the target table's current schema
/// (<c>tables.get</c>), producing the mismatch lines <c>PZBQ0304</c> lists verbatim. Read-only: this
/// never decides whether to create or evolve anything, only whether the two schemas can coexist as
/// they stand.</summary>
internal static class BqTargetSchema
{
    /// <summary>One line per incompatibility, in <paramref name="wanted"/> column order; empty when
    /// every wanted column exists in <paramref name="existing"/> with a compatible type and mode. A
    /// column present only in <paramref name="existing"/> is never mentioned -- it is left alone by
    /// every write mode this connector supports (NULL on append/merge inserts, kept on merge
    /// updates), so an extra target column is not a mismatch.</summary>
    public static IReadOnlyList<string> Diff(BqTableSchema existing, BqTableSchema wanted)
    {
        var mismatches = new List<string>();
        DiffFields(existing.Fields ?? [], wanted.Fields ?? [], prefix: "", mismatches);
        return mismatches;
    }

    // Recurses into RECORD fields, naming a nested mismatch by its dotted path. A subfield present
    // only in the target is left alone for the same reason a top-level extra column is.
    private static void DiffFields(
        IReadOnlyList<BqFieldSchema> existing, IReadOnlyList<BqFieldSchema> wanted, string prefix, List<string> mismatches)
    {
        var existingByName = new Dictionary<string, BqFieldSchema>(StringComparer.Ordinal);
        foreach (var field in existing)
        {
            if (field.Name is { } name)
            {
                existingByName[name] = field;
            }
        }

        foreach (var wantedField in wanted)
        {
            var name = prefix + wantedField.Name!;
            if (!existingByName.TryGetValue(wantedField.Name!, out var existingField))
            {
                mismatches.Add($"column '{name}' is missing from the target table");
                continue;
            }

            // A scalar write cannot land in a REPEATED column, nor an array in a scalar one; REQUIRED
            // is fine -- every mapped column is NULLABLE, but a target that additionally forbids null
            // is still a legal write target as long as the data never actually carries a null there.
            var existingRepeated = string.Equals(existingField.Mode, "REPEATED", StringComparison.Ordinal);
            var wantedRepeated = string.Equals(wantedField.Mode, "REPEATED", StringComparison.Ordinal);
            if (existingRepeated != wantedRepeated)
            {
                mismatches.Add(existingRepeated
                    ? $"column '{name}' is REPEATED in the target table; the write schema needs a scalar column"
                    : $"column '{name}' is not REPEATED in the target table; the write schema needs a REPEATED column");
                continue;
            }

            var existingType = Canonical(existingField.Type);
            var wantedType = Canonical(wantedField.Type);
            if (!string.Equals(existingType, wantedType, StringComparison.Ordinal))
            {
                mismatches.Add(
                    $"column '{name}' has type '{existingField.Type}' in the target table; the write schema needs '{wantedField.Type}'");
                continue;
            }

            if (wantedType == "RECORD")
            {
                DiffFields(existingField.Fields ?? [], wantedField.Fields ?? [], name + ".", mismatches);
                continue;
            }

            // A target created without an explicit precision/scale (tables.get then reports neither)
            // is compared on type alone -- BigQuery's own NUMERIC/BIGNUMERIC default still accepts
            // the write, and a stored default is not necessarily readable back from tables.get.
            if (existingField.Precision is not null
                && (existingField.Precision != wantedField.Precision || existingField.Scale != wantedField.Scale))
            {
                mismatches.Add(
                    $"column '{name}' is {existingField.Type}({existingField.Precision},{existingField.Scale}) in the target table; "
                    + $"the write schema needs {wantedField.Type}({wantedField.Precision},{wantedField.Scale})");
            }
        }
    }

    // tables.get reports the legacy spelling (INTEGER/FLOAT/BOOLEAN); this connector's own schema
    // map emits the same legacy spelling, but a caller may still hand either side the modern one
    // (INT64/FLOAT64/BOOL), so both directions are normalized before comparing.
    private static string Canonical(string? type) => type switch
    {
        "INT64" => "INTEGER",
        "FLOAT64" => "FLOAT",
        "BOOL" => "BOOLEAN",
        "STRUCT" => "RECORD",
        null => "",
        _ => type,
    };
}
