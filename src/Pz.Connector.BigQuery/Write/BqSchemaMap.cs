using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>Maps an Arrow schema to the BigQuery schema a load job or <c>tables.insert</c> call
/// carries on the wire. Emits BigQuery's legacy type spellings (<c>INTEGER</c>/<c>FLOAT</c>/
/// <c>BOOLEAN</c>, never <c>INT64</c>/<c>FLOAT64</c>/<c>BOOL</c>): the Storage API used elsewhere in
/// this connector accepts only the legacy names, so a write-side schema built with the modern ones
/// would be silently unusable the moment it needed to round-trip through that same surface. Every
/// field comes back <c>NULLABLE</c> -- an Arrow non-null column still lands through the NDJSON load
/// path, which BigQuery accepts against a nullable target column, and a stricter target-schema
/// requirement is instead the job of <see cref="BqTargetSchema"/> comparing against what already
/// exists.</summary>
internal static class BqSchemaMap
{
    /// <summary>The session-monotonic ordering column merge mode appends to every staged row.
    /// Reserved: a write schema declaring a column of this name is refused before this map ever
    /// runs (<c>PZBQ0302</c>).</summary>
    public const string SequenceColumn = "_pz_seq";

    private const int MaxNumericScale = 9;
    private const int MaxBigNumericPrecision = 76;
    private const int MaxBigNumericScale = 38;

    public static BqTableSchema ToBigQuery(Schema arrow, bool withSequence, string output)
    {
        var fields = new List<BqFieldSchema>(arrow.FieldsList.Count + (withSequence ? 1 : 0));
        foreach (var field in arrow.FieldsList)
        {
            fields.Add(MapField(field, output));
        }

        if (withSequence)
        {
            fields.Add(new BqFieldSchema(SequenceColumn, "INTEGER", "NULLABLE", null, null, null));
        }

        return new BqTableSchema(fields.ToArray());
    }

    public static BqFieldSchema MapField(Field f, string output)
    {
        var (type, precision, scale) = MapType(f.DataType, f.Name, output);
        return new BqFieldSchema(f.Name, type, "NULLABLE", precision, scale, null);
    }

    private static (string Type, string? Precision, string? Scale) MapType(IArrowType type, string column, string output)
    {
        switch (type)
        {
            case Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type:
                return ("INTEGER", null, null);

            // INT64 cannot hold the full UInt64 range (up to 2^64 - 1); NUMERIC's default (38, 9)
            // precision covers it with room to spare, so no explicit precision/scale is needed.
            case UInt64Type:
                return ("NUMERIC", null, null);

            case FloatType or DoubleType:
                return ("FLOAT", null, null);

            // Decimal128Type/Decimal256Type both derive from FixedSizeBinaryType, so each must be
            // fully resolved here -- matched to a size or explicitly refused -- before falling
            // through to the generic FixedSizeBinaryType case below, which would otherwise catch an
            // out-of-range decimal as plain BYTES.
            case Decimal128Type d when d.Precision <= 38 && d.Scale <= MaxNumericScale:
                return ("NUMERIC", Digits(d.Precision), Digits(d.Scale));

            case Decimal128Type d when d.Precision <= MaxBigNumericPrecision && d.Scale <= MaxBigNumericScale:
                return ("BIGNUMERIC", Digits(d.Precision), Digits(d.Scale));

            case Decimal128Type:
                throw Unsupported(column, type, output);

            case Decimal256Type d when d.Precision <= MaxBigNumericPrecision && d.Scale <= MaxBigNumericScale:
                return ("BIGNUMERIC", Digits(d.Precision), Digits(d.Scale));

            case Decimal256Type:
                throw Unsupported(column, type, output);

            // Decimal32Type/Decimal64Type share the same FixedSizeBinaryType ancestry as
            // Decimal128Type/Decimal256Type above, and would otherwise fall into the generic
            // FixedSizeBinaryType case below and be silently mapped to BYTES. The engine only ever
            // hands this connector Decimal128 for a decimal column, so narrower decimals are refused
            // rather than given a NUMERIC/BIGNUMERIC mapping of their own.
            case Decimal32Type or Decimal64Type:
                throw Unsupported(column, type, output);

            case StringType or LargeStringType:
                return ("STRING", null, null);

            case BinaryType or LargeBinaryType or FixedSizeBinaryType:
                return ("BYTES", null, null);

            case BooleanType:
                return ("BOOLEAN", null, null);

            case Date32Type or Date64Type:
                return ("DATE", null, null);

            case TimestampType t:
                return (string.IsNullOrEmpty(t.Timezone) ? "DATETIME" : "TIMESTAMP", null, null);

            case Time32Type or Time64Type:
                return ("TIME", null, null);

            default:
                throw Unsupported(column, type, output);
        }
    }

    private static string Digits(int value) => value.ToString(CultureInfo.InvariantCulture);

    // BqRedactor.None: this message names only an output, a column, and an Arrow type name, never a
    // value that could carry a secret, so no caller-supplied redactor is needed.
    private static PzConnectorException Unsupported(string column, IArrowType type, string output) =>
        new(BqCodes.Message(BqCodes.Write_UnsupportedArrowType, BqRedactor.None,
                $"{output}: column '{column}' has unsupported Arrow type '{type.Name}'"),
            isTransient: false);
}
