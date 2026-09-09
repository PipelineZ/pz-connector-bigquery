using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqSchemaMapTests
{
    private static Field F(string name, IArrowType type, bool nullable = true) => new(name, type, nullable);

    [Theory]
    [InlineData(typeof(Int8Type), "INTEGER")]
    [InlineData(typeof(Int16Type), "INTEGER")]
    [InlineData(typeof(Int32Type), "INTEGER")]
    [InlineData(typeof(Int64Type), "INTEGER")]
    [InlineData(typeof(UInt8Type), "INTEGER")]
    [InlineData(typeof(UInt16Type), "INTEGER")]
    [InlineData(typeof(UInt32Type), "INTEGER")]
    [InlineData(typeof(FloatType), "FLOAT")]
    [InlineData(typeof(DoubleType), "FLOAT")]
    [InlineData(typeof(StringType), "STRING")]
    [InlineData(typeof(LargeStringType), "STRING")]
    [InlineData(typeof(BinaryType), "BYTES")]
    [InlineData(typeof(LargeBinaryType), "BYTES")]
    [InlineData(typeof(BooleanType), "BOOLEAN")]
    [InlineData(typeof(Date32Type), "DATE")]
    [InlineData(typeof(Date64Type), "DATE")]
    public void Simple_arrow_types_map_to_legacy_wire_names(Type arrowType, string expectedBqType)
    {
        var type = (IArrowType)Activator.CreateInstance(arrowType)!;
        var field = BqSchemaMap.MapField(F("c", type), "target");

        Assert.Equal(expectedBqType, field.Type);
        Assert.Equal("NULLABLE", field.Mode);
        Assert.Null(field.Precision);
        Assert.Null(field.Scale);
    }

    [Fact]
    public void FixedSizeBinary_maps_to_bytes()
    {
        var field = BqSchemaMap.MapField(F("c", new FixedSizeBinaryType(16)), "target");
        Assert.Equal("BYTES", field.Type);
    }

    [Fact]
    public void Time32_maps_to_time()
    {
        var field = BqSchemaMap.MapField(F("c", new Time32Type(TimeUnit.Millisecond)), "target");
        Assert.Equal("TIME", field.Type);
    }

    [Fact]
    public void Time64_maps_to_time()
    {
        var field = BqSchemaMap.MapField(F("c", new Time64Type(TimeUnit.Microsecond)), "target");
        Assert.Equal("TIME", field.Type);
    }

    [Fact]
    public void UInt64_maps_to_numeric_without_explicit_precision()
    {
        var field = BqSchemaMap.MapField(F("c", UInt64Type.Default), "target");

        Assert.Equal("NUMERIC", field.Type);
        Assert.Null(field.Precision);
        Assert.Null(field.Scale);
    }

    [Fact]
    public void Decimal128_within_numeric_range_maps_to_numeric_with_precision_and_scale()
    {
        var field = BqSchemaMap.MapField(F("c", new Decimal128Type(38, 9)), "target");

        Assert.Equal("NUMERIC", field.Type);
        Assert.Equal("38", field.Precision);
        Assert.Equal("9", field.Scale);
    }

    [Fact]
    public void Decimal128_beyond_numeric_scale_maps_to_bignumeric()
    {
        var field = BqSchemaMap.MapField(F("c", new Decimal128Type(38, 12)), "target");

        Assert.Equal("BIGNUMERIC", field.Type);
        Assert.Equal("38", field.Precision);
        Assert.Equal("12", field.Scale);
    }

    [Fact]
    public void Decimal256_within_bignumeric_range_maps_to_bignumeric()
    {
        var field = BqSchemaMap.MapField(F("c", new Decimal256Type(76, 38)), "target");

        Assert.Equal("BIGNUMERIC", field.Type);
        Assert.Equal("76", field.Precision);
        Assert.Equal("38", field.Scale);
    }

    [Fact]
    public void Decimal256_beyond_bignumeric_range_is_PZBQ0303()
    {
        var ex = Assert.Throws<PzConnectorException>(() => BqSchemaMap.MapField(F("c", new Decimal256Type(77, 39)), "target"));
        Assert.Contains("PZBQ0303", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Timestamp_with_timezone_maps_to_timestamp()
    {
        var field = BqSchemaMap.MapField(F("c", new TimestampType(TimeUnit.Microsecond, "+00:00")), "target");
        Assert.Equal("TIMESTAMP", field.Type);
    }

    [Fact]
    public void Timestamp_without_timezone_maps_to_datetime()
    {
        var field = BqSchemaMap.MapField(F("c", new TimestampType(TimeUnit.Microsecond, timezone: (string?)null)), "target");
        Assert.Equal("DATETIME", field.Type);
    }

    [Fact]
    public void Every_field_is_nullable_regardless_of_arrow_nullability()
    {
        var field = BqSchemaMap.MapField(F("c", Int64Type.Default, nullable: false), "target");
        Assert.Equal("NULLABLE", field.Mode);
    }

    [Fact]
    public void List_type_is_refused_as_PZBQ0303_naming_the_column()
    {
        var ex = Assert.Throws<PzConnectorException>(() =>
            BqSchemaMap.MapField(F("tags", new ListType(Int64Type.Default)), "target"));

        Assert.Contains("PZBQ0303", ex.Message, StringComparison.Ordinal);
        Assert.Contains("tags", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Struct_type_is_refused_as_PZBQ0303()
    {
        var ex = Assert.Throws<PzConnectorException>(() =>
            BqSchemaMap.MapField(F("c", new StructType([new Field("member", Int64Type.Default, true)])), "target"));
        Assert.Contains("PZBQ0303", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ToBigQuery_maps_every_field_in_schema_order()
    {
        var schema = new Schema(new[] { F("id", Int64Type.Default), F("name", StringType.Default) }, null);
        var result = BqSchemaMap.ToBigQuery(schema, withSequence: false, "target");

        Assert.NotNull(result.Fields);
        Assert.Equal(2, result.Fields!.Length);
        Assert.Equal("id", result.Fields[0].Name);
        Assert.Equal("INTEGER", result.Fields[0].Type);
        Assert.Equal("name", result.Fields[1].Name);
        Assert.Equal("STRING", result.Fields[1].Type);
    }

    [Fact]
    public void ToBigQuery_appends_sequence_column_when_requested()
    {
        var schema = new Schema(new[] { F("id", Int64Type.Default) }, null);
        var result = BqSchemaMap.ToBigQuery(schema, withSequence: true, "target");

        Assert.Equal(2, result.Fields!.Length);
        Assert.Equal(BqSchemaMap.SequenceColumn, result.Fields[1].Name);
        Assert.Equal("INTEGER", result.Fields[1].Type);
        Assert.Equal("NULLABLE", result.Fields[1].Mode);
    }

    [Fact]
    public void SequenceColumn_constant_is_pz_seq()
    {
        Assert.Equal("_pz_seq", BqSchemaMap.SequenceColumn);
    }
}
