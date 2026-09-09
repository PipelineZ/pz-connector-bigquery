using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqRowRestrictionTests
{
    private static Schema SchemaOf(string name, IArrowType type) =>
        new(new[] { new Field(name, type, nullable: true) }, null);

    private static DatasetSpec Spec(string? cursor = null, string? value = null, string? upper = null, bool lowerInclusive = false) =>
        new("bq", "sales.orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = cursor,
            WatermarkValue = value,
            WatermarkUpperBound = upper,
            WatermarkLowerInclusive = lowerInclusive,
        };

    [Fact]
    public void No_predicate_and_no_watermark_yields_null()
    {
        var result = BqRowRestriction.Build(ReadHints.None, Spec(), SchemaOf("c", Int64Type.Default), BqRedactor.None);
        Assert.Null(result);
    }

    [Fact]
    public void Predicate_only_is_self_parenthesized()
    {
        var hints = new ReadHints(PredicateSql: "x > 1");
        var result = BqRowRestriction.Build(hints, Spec(), SchemaOf("c", Int64Type.Default), BqRedactor.None);
        Assert.Equal("(x > 1)", result);
    }

    [Fact]
    public void Lower_bound_is_strict_by_default()
    {
        var spec = Spec(cursor: "c", value: "3");
        var result = BqRowRestriction.Build(ReadHints.None, spec, SchemaOf("c", Int64Type.Default), BqRedactor.None);
        Assert.Equal("(`c` > 3)", result);
    }

    [Fact]
    public void Lower_bound_is_inclusive_when_requested()
    {
        var spec = Spec(cursor: "c", value: "3", lowerInclusive: true);
        var result = BqRowRestriction.Build(ReadHints.None, spec, SchemaOf("c", Int64Type.Default), BqRedactor.None);
        Assert.Equal("(`c` >= 3)", result);
    }

    [Fact]
    public void Upper_bound_alone()
    {
        var spec = Spec(cursor: "c", upper: "7");
        var result = BqRowRestriction.Build(ReadHints.None, spec, SchemaOf("c", Int64Type.Default), BqRedactor.None);
        Assert.Equal("(`c` <= 7)", result);
    }

    [Fact]
    public void Cursor_set_but_value_null_yields_no_bound_term()
    {
        var spec = Spec(cursor: "c");
        var result = BqRowRestriction.Build(ReadHints.None, spec, SchemaOf("c", Int64Type.Default), BqRedactor.None);
        Assert.Null(result);
    }

    [Fact]
    public void Predicate_and_both_bounds_join_in_order_with_parentheses()
    {
        var hints = new ReadHints(PredicateSql: "pred");
        var spec = Spec(cursor: "c", value: "3", upper: "7");
        var result = BqRowRestriction.Build(hints, spec, SchemaOf("c", Int64Type.Default), BqRedactor.None);
        Assert.Equal("(pred) and (`c` > 3) and (`c` <= 7)", result);
    }

    [Fact]
    public void Missing_cursor_column_is_PZBQ0202()
    {
        var spec = Spec(cursor: "missing", value: "3");
        var ex = Assert.Throws<PzConnectorException>(() =>
            BqRowRestriction.Build(ReadHints.None, spec, SchemaOf("c", Int64Type.Default), BqRedactor.None));
        Assert.Contains("PZBQ0202", ex.Message);
        Assert.Contains("missing", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Unsupported_cursor_type_is_PZBQ0203()
    {
        var spec = Spec(cursor: "c", value: "3");
        var ex = Assert.Throws<PzConnectorException>(() =>
            BqRowRestriction.Build(ReadHints.None, spec, SchemaOf("c", new ListType(Int64Type.Default)), BqRedactor.None));
        Assert.Contains("PZBQ0203", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Theory]
    [InlineData("integer", "42")]
    [InlineData("double", "42.5")]
    public void Numeric_literals_pass_through_as_is(string kind, string canonical)
    {
        IArrowType type = kind == "integer" ? Int64Type.Default : DoubleType.Default;
        Assert.Equal(canonical, BqRowRestriction.Literal(type, canonical, "c"));
    }

    [Fact]
    public void Decimal128_is_quoted_as_numeric()
    {
        Assert.Equal("NUMERIC '1.50'", BqRowRestriction.Literal(new Decimal128Type(10, 2), "1.50", "c"));
    }

    [Fact]
    public void Decimal256_is_quoted_as_bignumeric()
    {
        Assert.Equal("BIGNUMERIC '1.50'", BqRowRestriction.Literal(new Decimal256Type(20, 2), "1.50", "c"));
    }

    [Fact]
    public void Date32_is_quoted_as_date()
    {
        Assert.Equal("DATE '2024-01-02'", BqRowRestriction.Literal(Date32Type.Default, "2024-01-02", "c"));
    }

    [Fact]
    public void Timestamp_with_timezone_becomes_a_utc_timestamp_literal()
    {
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");
        Assert.Equal("TIMESTAMP '2024-01-02 03:04:05.678900+00'",
            BqRowRestriction.Literal(type, "2024-01-02T03:04:05.678900", "c"));
    }

    [Fact]
    public void Timestamp_without_timezone_becomes_a_datetime_literal()
    {
        var type = new TimestampType(TimeUnit.Microsecond, timezone: (string?)null);
        Assert.Equal("DATETIME '2024-01-02T03:04:05.678900'",
            BqRowRestriction.Literal(type, "2024-01-02T03:04:05.678900", "c"));
    }

    [Fact]
    public void String_literal_escapes_backslash_and_quote()
    {
        Assert.Equal(@"'a\\b\'c'", BqRowRestriction.Literal(StringType.Default, "a\\b'c", "c"));
    }

    [Theory]
    [InlineData("not-a-number")]
    [InlineData("1.2.3")]
    public void Numeric_canonical_mismatch_is_PZBQ0203(string canonical)
    {
        var ex = Assert.Throws<PzConnectorException>(() => BqRowRestriction.Literal(Int64Type.Default, canonical, "c"));
        Assert.Contains("PZBQ0203", ex.Message);
    }

    [Fact]
    public void Date_canonical_mismatch_is_PZBQ0203()
    {
        var ex = Assert.Throws<PzConnectorException>(() => BqRowRestriction.Literal(Date32Type.Default, "01/02/2024", "c"));
        Assert.Contains("PZBQ0203", ex.Message);
    }

    [Fact]
    public void Timestamp_canonical_mismatch_is_PZBQ0203()
    {
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");
        var ex = Assert.Throws<PzConnectorException>(() => BqRowRestriction.Literal(type, "not-a-timestamp", "c"));
        Assert.Contains("PZBQ0203", ex.Message);
    }

    [Fact]
    public void Unsupported_type_is_PZBQ0203()
    {
        var ex = Assert.Throws<PzConnectorException>(() => BqRowRestriction.Literal(new ListType(Int64Type.Default), "x", "c"));
        Assert.Contains("PZBQ0203", ex.Message);
    }
}
