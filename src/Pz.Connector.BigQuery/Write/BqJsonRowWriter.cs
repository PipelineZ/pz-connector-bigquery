using System.Globalization;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Pz.Connector.BigQuery;

/// <summary>Encodes one <see cref="RecordBatch"/> as NDJSON for the spool: one JSON object per row,
/// field order matching the schema, plus a trailing <see cref="BqSchemaMap.SequenceColumn"/> in merge
/// mode. Every value is copied out of the engine-owned batch during the call -- nothing from it is
/// retained past <see cref="Write"/> returning. Column dispatch is by concrete array type, never
/// through a boxing <c>object</c> value, so the only per-row allocation is whatever the single shared
/// <see cref="Utf8JsonWriter"/> itself buffers.</summary>
internal sealed class BqJsonRowWriter(Schema schema, bool withSequence)
{
    // SkipValidation: without it, Utf8JsonWriter refuses a second top-level value once one row's
    // object has closed -- NDJSON is many top-level values sharing one writer, one per line.
    private static readonly JsonWriterOptions WriterOptions = new() { SkipValidation = true };

    /// <summary>Returns the next sequence number a following call should start from, so
    /// <see cref="BqSchemaMap.SequenceColumn"/> stays monotonic across every file in one session
    /// regardless of how the caller chunks batches into <see cref="Write"/> calls.</summary>
    public long Write(RecordBatch batch, Stream ndjson, long nextSequence)
    {
        var fields = schema.FieldsList;
        var columns = new IArrowArray[fields.Count];
        for (var i = 0; i < fields.Count; i++)
        {
            columns[i] = batch.Column(i);
        }

        using var writer = new Utf8JsonWriter(ndjson, WriterOptions);
        for (var row = 0; row < batch.Length; row++)
        {
            writer.WriteStartObject();
            for (var col = 0; col < fields.Count; col++)
            {
                writer.WritePropertyName(fields[col].Name);
                WriteValue(writer, fields[col], columns[col], row);
            }

            if (withSequence)
            {
                writer.WritePropertyName(BqSchemaMap.SequenceColumn);
                writer.WriteNumberValue(nextSequence + row);
            }

            writer.WriteEndObject();

            // The writer buffers internally; it must be flushed to the stream before a raw '\n' is
            // written straight to that same stream, or the newline would land ahead of buffered
            // object bytes still in flight. Reset() then clears the "already wrote a top-level
            // value" bookkeeping SkipValidation otherwise leaves in place -- without it, the next
            // object would open with a leading comma, as if it were another element of the same
            // array rather than its own NDJSON line.
            writer.Flush();
            ndjson.WriteByte((byte)'\n');
            writer.Reset();
        }

        return nextSequence + batch.Length;
    }

    private static void WriteValue(Utf8JsonWriter writer, Field field, IArrowArray array, int row)
    {
        if (array.IsNull(row))
        {
            writer.WriteNullValue();
            return;
        }

        switch (array)
        {
            case Int8Array a: writer.WriteNumberValue(a.GetValue(row).GetValueOrDefault()); break;
            case Int16Array a: writer.WriteNumberValue(a.GetValue(row).GetValueOrDefault()); break;
            case Int32Array a: writer.WriteNumberValue(a.GetValue(row).GetValueOrDefault()); break;
            case Int64Array a: writer.WriteNumberValue(a.GetValue(row).GetValueOrDefault()); break;
            case UInt8Array a: writer.WriteNumberValue(a.GetValue(row).GetValueOrDefault()); break;
            case UInt16Array a: writer.WriteNumberValue(a.GetValue(row).GetValueOrDefault()); break;
            case UInt32Array a: writer.WriteNumberValue(a.GetValue(row).GetValueOrDefault()); break;

            // INT64 cannot hold the full UInt64 range; BigQuery's NUMERIC column (BqSchemaMap) wants
            // a plain digit string instead of a JSON number.
            case UInt64Array a: writer.WriteStringValue(a.GetValue(row).GetValueOrDefault().ToString(CultureInfo.InvariantCulture)); break;

            case FloatArray a: WriteFloatingPoint(writer, a.GetValue(row).GetValueOrDefault()); break;
            case DoubleArray a: WriteFloatingPoint(writer, a.GetValue(row).GetValueOrDefault()); break;

            case Decimal128Array a: writer.WriteStringValue(a.GetSqlDecimal(row)!.Value.ToString()); break;
            case Decimal256Array a: writer.WriteStringValue(a.GetString(row)); break;

            // Decimal32Array/Decimal64Array share FixedSizeBinaryArray's ancestry, same as
            // Decimal128Array/Decimal256Array above; BqSchemaMap already refuses the corresponding
            // field types, so this is unreachable in practice, but the guard stays here too rather
            // than let a decimal instance ever fall into the generic FixedSizeBinaryArray case below
            // and be base64-encoded as raw bytes instead of refused.
            case Decimal32Array or Decimal64Array:
                throw new InvalidOperationException(
                    $"column '{field.Name}': array type '{array.GetType().Name}' has no NDJSON encoding "
                    + "-- it should have been refused by BqSchemaMap before reaching the row writer");

            case BooleanArray a: writer.WriteBooleanValue(a.GetValue(row).GetValueOrDefault()); break;

            // LargeStringArray/StringArray each extend their non-string Binary counterpart, so they
            // must be checked before it -- otherwise the binary case below would catch them first
            // and base64-encode text instead of writing it as a JSON string.
            case LargeStringArray a: writer.WriteStringValue(a.GetString(row)); break;
            case StringArray a: writer.WriteStringValue(a.GetString(row)); break;

            case LargeBinaryArray a: writer.WriteBase64StringValue(a.GetBytes(row)); break;
            case FixedSizeBinaryArray a: writer.WriteBase64StringValue(a.GetBytes(row)); break;
            case BinaryArray a: writer.WriteBase64StringValue(a.GetBytes(row)); break;

            case Date32Array a: WriteDate(writer, a.GetDateOnly(row)!.Value); break;
            case Date64Array a: WriteDate(writer, a.GetDateOnly(row)!.Value); break;

            case TimestampArray a: WriteTimestamp(writer, (TimestampType)field.DataType, a.GetTimestamp(row)!.Value); break;

            case Time32Array a: WriteTime(writer, a.GetTime(row)!.Value); break;
            case Time64Array a: WriteTime(writer, a.GetTime(row)!.Value); break;

            // Every other Arrow type is refused at schema-map time (PZBQ0303); reaching here means a
            // caller handed this writer a batch whose schema was never checked against BqSchemaMap.
            default:
                throw new InvalidOperationException(
                    $"column '{field.Name}': array type '{array.GetType().Name}' has no NDJSON encoding "
                    + "-- it should have been refused by BqSchemaMap before reaching the row writer");
        }
    }

    // A dedicated float overload, rather than widening to double first: double.ToString() on a
    // widened float carries the extra binary-to-decimal noise of the widening itself (1.1f becomes
    // 1.100000023841858 as a double), where Utf8JsonWriter.WriteNumberValue(float) instead writes
    // the shortest round-trippable decimal for the float value actually stored.
    private static void WriteFloatingPoint(Utf8JsonWriter writer, float value)
    {
        if (float.IsNaN(value))
        {
            writer.WriteStringValue("NaN");
            return;
        }

        if (float.IsPositiveInfinity(value))
        {
            writer.WriteStringValue("Infinity");
            return;
        }

        if (float.IsNegativeInfinity(value))
        {
            writer.WriteStringValue("-Infinity");
            return;
        }

        writer.WriteNumberValue(value);
    }

    private static void WriteFloatingPoint(Utf8JsonWriter writer, double value)
    {
        if (double.IsNaN(value))
        {
            writer.WriteStringValue("NaN");
            return;
        }

        if (double.IsPositiveInfinity(value))
        {
            writer.WriteStringValue("Infinity");
            return;
        }

        if (double.IsNegativeInfinity(value))
        {
            writer.WriteStringValue("-Infinity");
            return;
        }

        writer.WriteNumberValue(value);
    }

    private static void WriteDate(Utf8JsonWriter writer, DateOnly date) =>
        writer.WriteStringValue(date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));

    private static void WriteTime(Utf8JsonWriter writer, TimeOnly time) =>
        writer.WriteStringValue(time.ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture));

    // GetTimestamp already normalizes any storage unit (second/millisecond/microsecond/nanosecond)
    // into one DateTimeOffset with a zero UTC offset, tz-aware or not -- the field's own type is
    // consulted only to decide TIMESTAMP-with-Z vs DATETIME-without, never to reinterpret the value.
    private static void WriteTimestamp(Utf8JsonWriter writer, TimestampType type, DateTimeOffset value)
    {
        var text = value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture);
        writer.WriteStringValue(string.IsNullOrEmpty(type.Timezone) ? text : text + "Z");
    }
}
