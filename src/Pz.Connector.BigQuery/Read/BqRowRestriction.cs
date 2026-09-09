using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>Builds the GoogleSQL <c>WHERE</c> clause a Storage Read API session pushes down:
/// caller-supplied predicate SQL plus the incremental watermark's lower/upper bounds, one term per
/// condition that applies. GoogleSQL, unlike DuckDB, refuses an untyped string literal against a
/// typed column (a DATE column rejects a bare quoted string), so every bound literal carries the
/// type keyword its column's Arrow type calls for.</summary>
internal static partial class BqRowRestriction
{
    private static readonly Regex NumericPattern = NumericRegex();
    private static readonly Regex DatePattern = DateRegex();
    private static readonly Regex TimestampPattern = TimestampRegex();

    /// <summary>Terms in order: predicate, lower bound, upper bound -- each self-parenthesized and
    /// joined with " and ". <c>null</c> when none apply, so a caller can skip the clause entirely
    /// rather than pushing an empty <c>WHERE</c>.</summary>
    public static string? Build(ReadHints hints, DatasetSpec spec, Schema arrowSchema, BqRedactor redactor)
    {
        var terms = new List<string>();

        // DuckDB's own SQL parser -- the source of PredicateSql -- emits "ident" (double quotes) for
        // any identifier that needs quoting, but GoogleSQL reads a double-quoted token as a STRING
        // LITERAL, not an identifier. Pushing such a predicate verbatim does not fail loudly: it
        // silently compares the wrong thing (or a typed column against a string) and can return wrong
        // rows. Omitting the term here is always safe -- the engine still filters the same predicate
        // locally against the unfiltered read -- so a predicate containing '"' is simply not pushed.
        if (!string.IsNullOrEmpty(hints.PredicateSql) && !hints.PredicateSql.Contains('"'))
        {
            terms.Add($"({hints.PredicateSql})");
        }

        if (spec.WatermarkCursor is { } cursor && (spec.WatermarkValue is not null || spec.WatermarkUpperBound is not null))
        {
            var field = arrowSchema.GetFieldByName(cursor);
            if (field is null)
            {
                throw new PzConnectorException(
                    BqCodes.Message(BqCodes.Read_CursorColumnNotInSchema, redactor,
                        $"watermark cursor column '{cursor}' is not in the read schema"),
                    isTransient: false);
            }

            var quotedCursor = TableRef.QuoteIdentifier(cursor);

            if (spec.WatermarkValue is { } lowerValue)
            {
                var op = spec.WatermarkLowerInclusive ? ">=" : ">";
                terms.Add($"({quotedCursor} {op} {Literal(field.DataType, lowerValue, cursor)})");
            }

            if (spec.WatermarkUpperBound is { } upperValue)
            {
                terms.Add($"({quotedCursor} <= {Literal(field.DataType, upperValue, cursor)})");
            }
        }

        return terms.Count == 0 ? null : string.Join(" and ", terms);
    }

    /// <summary>Renders one watermark bound value as a typed GoogleSQL literal. <paramref name="canonical"/>
    /// is the engine's own text form of the value -- trusted to be digits or ISO text, but still
    /// checked against the shape each type accepts before it reaches a query string.</summary>
    public static string Literal(IArrowType type, string canonical, string column)
    {
        switch (type)
        {
            case Int64Type or Int32Type or Int16Type or Int8Type
                or UInt64Type or UInt32Type or UInt16Type or UInt8Type
                or DoubleType or FloatType:
                RequireCanonical(NumericPattern, canonical, column, type);
                return canonical;

            case Decimal128Type:
                RequireCanonical(NumericPattern, canonical, column, type);
                return $"NUMERIC '{canonical}'";

            case Decimal256Type:
                RequireCanonical(NumericPattern, canonical, column, type);
                return $"BIGNUMERIC '{canonical}'";

            case Date32Type or Date64Type:
                RequireCanonical(DatePattern, canonical, column, type);
                return $"DATE '{canonical}'";

            case TimestampType timestampType:
                RequireCanonical(TimestampPattern, canonical, column, type);
                return string.IsNullOrEmpty(timestampType.Timezone)
                    ? $"DATETIME '{canonical}'"
                    : $"TIMESTAMP '{canonical.Replace('T', ' ')}+00'";

            case StringType:
                return $"'{EscapeStringLiteral(canonical)}'";

            default:
                throw UnsupportedType(column, type);
        }
    }

    private static void RequireCanonical(Regex pattern, string canonical, string column, IArrowType type)
    {
        if (!pattern.IsMatch(canonical))
        {
            throw UnsupportedType(column, type, "value not in canonical form");
        }
    }

    private static string EscapeStringLiteral(string value) =>
        value.Replace("\\", "\\\\").Replace("'", "\\'");

    // BqRedactor.None: these messages name only a column and an Arrow type name, never a value that
    // could carry a secret, so no caller-supplied redactor is needed.
    private static PzConnectorException UnsupportedType(string column, IArrowType type, string? detail = null) =>
        new(BqCodes.Message(BqCodes.Read_UnsupportedCursorType, BqRedactor.None,
                detail is null
                    ? $"watermark cursor column '{column}' has type '{type.Name}', which is not supported in a row restriction"
                    : $"watermark cursor column '{column}' (type '{type.Name}'): {detail}"),
            isTransient: false);

    [GeneratedRegex(@"^-?\d+(\.\d+)?$")]
    private static partial Regex NumericRegex();

    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}$")]
    private static partial Regex DateRegex();

    [GeneratedRegex(@"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?$")]
    private static partial Regex TimestampRegex();
}
