using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqJsonRowWriterTests
{
    private static Schema SchemaOf(params Field[] fields) => new(fields, null);

    private static string WriteToText(BqJsonRowWriter writer, RecordBatch batch, long nextSequence, out long returned)
    {
        using var stream = new MemoryStream();
        returned = writer.Write(batch, stream, nextSequence);
        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    [Fact]
    public void One_row_per_line_with_a_trailing_newline()
    {
        var schema = SchemaOf(new Field("id", Int64Type.Default, nullable: true));
        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        var batch = new RecordBatch(schema, [ids], 2);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"id\":1}\n{\"id\":2}\n", text);
    }

    [Fact]
    public void Null_cell_is_json_null()
    {
        var schema = SchemaOf(new Field("id", Int64Type.Default, nullable: true));
        var builder = new Int64Array.Builder();
        builder.AppendNull();
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"id\":null}\n", text);
    }

    [Fact]
    public void Uint64_is_written_as_a_digit_string()
    {
        var schema = SchemaOf(new Field("big", UInt64Type.Default, nullable: true));
        var batch = new RecordBatch(schema, [new UInt64Array.Builder().Append(18446744073709551615UL).Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"big\":\"18446744073709551615\"}\n", text);
    }

    [Theory]
    [InlineData(double.NaN, "NaN")]
    [InlineData(double.PositiveInfinity, "Infinity")]
    [InlineData(double.NegativeInfinity, "-Infinity")]
    public void Non_finite_doubles_are_written_as_strings(double value, string expected)
    {
        var schema = SchemaOf(new Field("d", DoubleType.Default, nullable: true));
        var batch = new RecordBatch(schema, [new DoubleArray.Builder().Append(value).Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal($"{{\"d\":\"{expected}\"}}\n", text);
    }

    [Fact]
    public void Finite_double_is_a_json_number()
    {
        var schema = SchemaOf(new Field("d", DoubleType.Default, nullable: true));
        var batch = new RecordBatch(schema, [new DoubleArray.Builder().Append(1.5).Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"d\":1.5}\n", text);
    }

    [Fact]
    public void Finite_float_is_written_with_its_own_shortest_round_trip_text()
    {
        // Widening 1.1f to double before formatting would produce "1.100000023841858" -- the
        // widening itself introduces that noise, since 1.1f is not exactly representable and its
        // nearest double is not the nearest double to the decimal 1.1.
        var schema = SchemaOf(new Field("f", FloatType.Default, nullable: true));
        var batch = new RecordBatch(schema, [new FloatArray.Builder().Append(1.1f).Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"f\":1.1}\n", text);
    }

    [Theory]
    [InlineData(float.NaN, "NaN")]
    [InlineData(float.PositiveInfinity, "Infinity")]
    [InlineData(float.NegativeInfinity, "-Infinity")]
    public void Non_finite_floats_are_written_as_strings(float value, string expected)
    {
        var schema = SchemaOf(new Field("f", FloatType.Default, nullable: true));
        var batch = new RecordBatch(schema, [new FloatArray.Builder().Append(value).Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal($"{{\"f\":\"{expected}\"}}\n", text);
    }

    [Fact]
    public void Decimal128_is_a_digit_string()
    {
        var type = new Decimal128Type(38, 9);
        var schema = SchemaOf(new Field("amount", type, nullable: true));
        var builder = new Decimal128Array.Builder(type);
        builder.Append(-1.5m);
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"amount\":\"-1.500000000\"}\n", text);
    }

    [Fact]
    public void Decimal256_is_a_digit_string()
    {
        var type = new Decimal256Type(76, 38);
        var schema = SchemaOf(new Field("amount", type, nullable: true));
        var builder = new Decimal256Array.Builder(type);
        builder.Append(123.456m);
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.StartsWith("{\"amount\":\"123.456", text);
    }

    // Decimal32Array shares FixedSizeBinaryArray's ancestry with Decimal128Array/Decimal256Array;
    // BqSchemaMap already refuses the corresponding field type (PZBQ0303), and the row writer must
    // never silently base64-encode its raw bytes if a batch somehow reaches it anyway.
    [Fact]
    public void Decimal32_column_throws_at_write_time_rather_than_base64_encoding_raw_bytes()
    {
        var type = new Decimal32Type(5, 2);
        var schema = SchemaOf(new Field("amount", type, nullable: true));
        var builder = new Decimal32Array.Builder(type);
        builder.Append(123.45m);
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        Assert.Throws<InvalidOperationException>(() =>
            new BqJsonRowWriter(schema, withSequence: false).Write(batch, new MemoryStream(), 0));
    }

    [Fact]
    public void Bytes_are_base64_encoded()
    {
        var schema = SchemaOf(new Field("b", BinaryType.Default, nullable: true));
        var builder = new BinaryArray.Builder();
        builder.Append(new byte[] { 1, 2, 3 });
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"b\":\"" + Convert.ToBase64String([1, 2, 3]) + "\"}\n", text);
    }

    [Fact]
    public void Boolean_is_a_json_boolean()
    {
        var schema = SchemaOf(new Field("flag", BooleanType.Default, nullable: true));
        var batch = new RecordBatch(schema, [new BooleanArray.Builder().Append(true).Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"flag\":true}\n", text);
    }

    [Fact]
    public void Date32_is_yyyy_mm_dd()
    {
        var schema = SchemaOf(new Field("d", Date32Type.Default, nullable: true));
        var builder = new Date32Array.Builder();
        builder.Append(new DateTime(2026, 1, 2));
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"d\":\"2026-01-02\"}\n", text);
    }

    [Fact]
    public void Timestamp_with_timezone_is_utc_with_trailing_z()
    {
        var type = new TimestampType(TimeUnit.Microsecond, "UTC");
        var schema = SchemaOf(new Field("ts", type, nullable: true));
        var builder = new TimestampArray.Builder(type);
        builder.Append(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(60));
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"ts\":\"2026-01-02T03:04:05.000006Z\"}\n", text);
    }

    [Fact]
    public void Timestamp_without_timezone_has_no_trailing_z()
    {
        var type = new TimestampType(TimeUnit.Microsecond, timezone: (string?)null);
        var schema = SchemaOf(new Field("ts", type, nullable: true));
        var builder = new TimestampArray.Builder(type);
        builder.Append(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(60));
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"ts\":\"2026-01-02T03:04:05.000006\"}\n", text);
    }

    [Fact]
    public void Time64_is_hh_mm_ss_ffffff()
    {
        var type = new Time64Type(TimeUnit.Microsecond);
        var schema = SchemaOf(new Field("t", type, nullable: true));
        var builder = new Time64Array.Builder(type);
        builder.Append(new TimeOnly(3, 4, 5).Add(TimeSpan.FromTicks(60)));
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"t\":\"03:04:05.000006\"}\n", text);
    }

    [Fact]
    public void String_column_is_a_json_string()
    {
        var schema = SchemaOf(new Field("name", StringType.Default, nullable: true));
        var batch = new RecordBatch(schema, [new StringArray.Builder().Append("hello").Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"name\":\"hello\"}\n", text);
    }

    [Fact]
    public void Sequence_column_is_appended_last_when_requested()
    {
        var schema = SchemaOf(new Field("id", Int64Type.Default, nullable: true));
        var batch = new RecordBatch(schema, [new Int64Array.Builder().Append(1).Build()], 1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: true), batch, 0, out _);

        Assert.Equal("{\"id\":1,\"_pz_seq\":0}\n", text);
    }

    [Fact]
    public void Sequence_continues_across_two_write_calls()
    {
        var schema = SchemaOf(new Field("id", Int64Type.Default, nullable: true));
        var writer = new BqJsonRowWriter(schema, withSequence: true);
        var batch1 = new RecordBatch(schema, [new Int64Array.Builder().Append(1).Append(2).Append(3).Build()], 3);
        var batch2 = new RecordBatch(schema, [new Int64Array.Builder().Append(4).Append(5).Append(6).Build()], 3);

        using var stream1 = new MemoryStream();
        var next = writer.Write(batch1, stream1, 0);
        Assert.Equal(3, next);
        Assert.Equal(
            "{\"id\":1,\"_pz_seq\":0}\n{\"id\":2,\"_pz_seq\":1}\n{\"id\":3,\"_pz_seq\":2}\n",
            System.Text.Encoding.UTF8.GetString(stream1.ToArray()));

        using var stream2 = new MemoryStream();
        next = writer.Write(batch2, stream2, next);
        Assert.Equal(6, next);
        Assert.Equal(
            "{\"id\":4,\"_pz_seq\":3}\n{\"id\":5,\"_pz_seq\":4}\n{\"id\":6,\"_pz_seq\":5}\n",
            System.Text.Encoding.UTF8.GetString(stream2.ToArray()));
    }

    [Fact]
    public void Multiple_columns_are_written_in_schema_order()
    {
        var schema = SchemaOf(
            new Field("id", Int64Type.Default, nullable: true),
            new Field("name", StringType.Default, nullable: true));
        var batch = new RecordBatch(
            schema,
            [new Int64Array.Builder().Append(7).Build(), new StringArray.Builder().Append("x").Build()],
            1);

        var text = WriteToText(new BqJsonRowWriter(schema, withSequence: false), batch, 0, out _);

        Assert.Equal("{\"id\":7,\"name\":\"x\"}\n", text);
    }

    [Fact]
    public void List_column_throws_at_write_time_rather_than_silently_writing_garbage()
    {
        var listType = new ListType(Int64Type.Default);
        var schema = SchemaOf(new Field("tags", listType, nullable: true));
        var builder = new ListArray.Builder(Int64Type.Default);
        builder.Append();
        var batch = new RecordBatch(schema, [builder.Build()], 1);

        Assert.Throws<InvalidOperationException>(() =>
            new BqJsonRowWriter(schema, withSequence: false).Write(batch, new MemoryStream(), 0));
    }
}
