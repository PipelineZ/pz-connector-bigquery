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

    // Decimal32Type/Decimal64Type derive from the same FixedSizeBinaryType ancestor as
    // Decimal128Type/Decimal256Type; the engine only ever hands this connector Decimal128 for a
    // decimal column, so these narrower decimals are refused rather than mapped to BYTES.
    [Fact]
    public void Decimal32_is_PZBQ0303()
    {
        var ex = Assert.Throws<PzConnectorException>(() => BqSchemaMap.MapField(F("amount", new Decimal32Type(5, 2)), "target"));

        Assert.Contains("PZBQ0303", ex.Message, StringComparison.Ordinal);
        Assert.Contains("amount", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Decimal64_is_PZBQ0303()
    {
        var ex = Assert.Throws<PzConnectorException>(() => BqSchemaMap.MapField(F("amount", new Decimal64Type(10, 2)), "target"));

        Assert.Contains("PZBQ0303", ex.Message, StringComparison.Ordinal);
        Assert.Contains("amount", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
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
    public void List_is_a_repeated_column_of_the_element_type()
    {
        var field = BqSchemaMap.MapField(F("tags", new ListType(Int64Type.Default)), "target");

        Assert.Equal("INTEGER", field.Type);
        Assert.Equal("REPEATED", field.Mode);
        Assert.Null(field.Fields);
    }

    [Fact]
    public void Fixed_size_list_is_a_repeated_column()
    {
        var field = BqSchemaMap.MapField(F("pair", new FixedSizeListType(Int32Type.Default, 2)), "target");

        Assert.Equal("INTEGER", field.Type);
        Assert.Equal("REPEATED", field.Mode);
    }

    [Fact]
    public void Struct_is_a_record_with_mapped_subfields()
    {
        var type = new StructType([new Field("n", Int32Type.Default, true), new Field("tags", new ListType(StringType.Default), true)]);

        var field = BqSchemaMap.MapField(F("rec", type), "target");

        Assert.Equal("RECORD", field.Type);
        Assert.Equal("NULLABLE", field.Mode);
        Assert.Equal(["n", "tags"], field.Fields!.Select(f => f.Name));
        Assert.Equal("INTEGER", field.Fields![0].Type);
        Assert.Equal("STRING", field.Fields![1].Type);
        Assert.Equal("REPEATED", field.Fields![1].Mode);
    }

    [Fact]
    public void List_of_struct_is_a_repeated_record()
    {
        var item = new StructType([new Field("k", StringType.Default, true)]);

        var field = BqSchemaMap.MapField(F("items", new ListType(item)), "target");

        Assert.Equal("RECORD", field.Type);
        Assert.Equal("REPEATED", field.Mode);
        Assert.Equal(["k"], field.Fields!.Select(f => f.Name));
    }

    [Fact]
    public void Map_is_a_repeated_record_of_key_and_value()
    {
        var field = BqSchemaMap.MapField(F("m", new MapType(StringType.Default, Int32Type.Default)), "target");

        Assert.Equal("RECORD", field.Type);
        Assert.Equal("REPEATED", field.Mode);
        Assert.Equal(["key", "value"], field.Fields!.Select(f => f.Name));
        Assert.Equal("STRING", field.Fields![0].Type);
        Assert.Equal("INTEGER", field.Fields![1].Type);
    }

    [Fact]
    public void Map_with_a_list_value_keeps_the_value_repeated()
    {
        var field = BqSchemaMap.MapField(F("m", new MapType(StringType.Default, new ListType(Int64Type.Default))), "target");

        Assert.Equal("REPEATED", field.Fields![1].Mode);
    }

    [Fact]
    public void List_of_list_is_refused_as_PZBQ0303_naming_the_column()
    {
        var ex = Assert.Throws<PzConnectorException>(() =>
            BqSchemaMap.MapField(F("grid", new ListType(new ListType(Int64Type.Default))), "target"));

        Assert.Contains("PZBQ0303", ex.Message, StringComparison.Ordinal);
        Assert.Contains("grid", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void List_of_map_is_refused_naming_the_nested_path()
    {
        var inner = new StructType([new Field("m", new ListType(new MapType(StringType.Default, Int32Type.Default)), true)]);

        var ex = Assert.Throws<PzConnectorException>(() => BqSchemaMap.MapField(F("rec", inner), "target"));

        Assert.Contains("PZBQ0303", ex.Message, StringComparison.Ordinal);
        Assert.Contains("rec.m", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Unsupported_leaf_inside_a_struct_is_refused_with_its_path()
    {
        var ex = Assert.Throws<PzConnectorException>(() =>
            BqSchemaMap.MapField(F("c", new StructType([new Field("d", DurationType.Millisecond, true)])), "target"));

        Assert.Contains("c.d", ex.Message, StringComparison.Ordinal);
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
