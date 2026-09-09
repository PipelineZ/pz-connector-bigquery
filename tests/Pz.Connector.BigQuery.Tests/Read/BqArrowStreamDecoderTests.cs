using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

public sealed class BqArrowStreamDecoderTests
{
    private static Schema SampleSchema() => new(
        new Field[]
        {
            new("id", Int64Type.Default, nullable: true),
            new("name", StringType.Default, nullable: true),
        },
        null);

    private static RecordBatch SampleBatch(Schema schema, long[] ids, string[] names)
    {
        var idArray = new Int64Array.Builder().AppendRange(ids).Build();
        var nameArray = new StringArray.Builder().AppendRange(names).Build();
        return new RecordBatch(schema, new IArrowArray[] { idArray, nameArray }, ids.Length);
    }

    // A writer's Dispose() never emits bytes on its own -- WriteStart()/WriteRecordBatch() are what
    // trigger the schema message, and WriteEnd() is what emits EOS. These helpers build exact byte
    // sequences so a test can hand the decoder precisely the framing it needs to prove.
    private static byte[] WriteSchemaOnly(Schema schema)
    {
        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            writer.WriteStart();
        }

        return stream.ToArray();
    }

    private static byte[] WriteFullStream(Schema schema, IEnumerable<RecordBatch> batches)
    {
        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
        {
            writer.WriteStart();
            foreach (var batch in batches)
            {
                writer.WriteRecordBatch(batch);
            }

            writer.WriteEnd();
        }

        return stream.ToArray();
    }

    // The lone RecordBatch message from a full schema+batch stream: everything from the second
    // 0xFFFFFFFF continuation marker onward, with the trailing EOS marker dropped.
    private static byte[] ExtractBatchMessageOnly(byte[] fullStreamBytes)
    {
        var markerPositions = new List<int>();
        for (var i = 0; i + 4 <= fullStreamBytes.Length; i++)
        {
            if (fullStreamBytes[i] == 0xFF && fullStreamBytes[i + 1] == 0xFF
                && fullStreamBytes[i + 2] == 0xFF && fullStreamBytes[i + 3] == 0xFF)
            {
                markerPositions.Add(i);
            }
        }

        Assert.True(markerPositions.Count >= 2, "expected at least a schema marker and a batch marker");
        var batchStart = markerPositions[1];

        // Drop the trailing 8-byte EOS marker (FF FF FF FF 00 00 00 00).
        var eosStart = fullStreamBytes.Length - 8;
        return fullStreamBytes[batchStart..eosStart];
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> ToAsyncEnumerable(IEnumerable<byte[]> blobs)
    {
        foreach (var blob in blobs)
        {
            await Task.Yield();
            yield return blob;
        }
    }

    private static async Task<List<RecordBatch>> CollectAsync(IAsyncEnumerable<RecordBatch> batches)
    {
        var result = new List<RecordBatch>();
        await foreach (var batch in batches)
        {
            result.Add(batch);
        }

        return result;
    }

    [Fact]
    public async Task BatchOnly_layout_decodes_a_lone_record_batch_message_prefixed_with_the_schema()
    {
        var schema = SampleSchema();
        var schemaBytes = WriteSchemaOnly(schema);
        var fullStream = WriteFullStream(schema, [SampleBatch(schema, [1, 2], ["a", "b"])]);
        var batchOnlyBlob = ExtractBatchMessageOnly(fullStream);

        var decoder = new BqArrowStreamDecoder(schemaBytes);
        var batches = await CollectAsync(decoder.DecodeAsync(ToAsyncEnumerable([batchOnlyBlob]), CancellationToken.None));

        Assert.Equal(BlobLayout.BatchOnly, decoder.Layout);
        Assert.Single(batches);
        var batch = batches[0];
        Assert.Equal(2, batch.Length);
        Assert.Equal(1L, ((Int64Array)batch.Column(0)).GetValue(0));
        Assert.Equal(2L, ((Int64Array)batch.Column(0)).GetValue(1));
        Assert.Equal("a", ((StringArray)batch.Column(1)).GetString(0));
        Assert.Equal("b", ((StringArray)batch.Column(1)).GetString(1));
    }

    [Fact]
    public async Task SelfContained_layout_decodes_full_streams_and_skips_the_empty_schema_batch()
    {
        var schema = SampleSchema();
        var emptyBatch = SampleBatch(schema, [], []);
        var schemaBlob = WriteFullStream(schema, [emptyBatch]);

        var blob1 = WriteFullStream(schema, [SampleBatch(schema, [10], ["x"])]);
        var blob2 = WriteFullStream(schema, [SampleBatch(schema, [20], ["y"])]);

        var decoder = new BqArrowStreamDecoder(schemaBlob);
        var batches = await CollectAsync(decoder.DecodeAsync(ToAsyncEnumerable([blob1, blob2]), CancellationToken.None));

        Assert.Equal(BlobLayout.SelfContained, decoder.Layout);
        Assert.Equal(2, batches.Count);
        Assert.Equal(10L, ((Int64Array)batches[0].Column(0)).GetValue(0));
        Assert.Equal("x", ((StringArray)batches[0].Column(1)).GetString(0));
        Assert.Equal(20L, ((Int64Array)batches[1].Column(0)).GetValue(0));
        Assert.Equal("y", ((StringArray)batches[1].Column(1)).GetString(0));
    }

    [Fact]
    public void ReadSchema_works_for_the_BatchOnly_schema_bytes()
    {
        var schema = SampleSchema();
        var schemaBytes = WriteSchemaOnly(schema);
        var decoder = new BqArrowStreamDecoder(schemaBytes);

        var read = decoder.ReadSchema();

        Assert.Equal(2, read.FieldsList.Count);
        Assert.Equal("id", read.FieldsList[0].Name);
        Assert.Equal("name", read.FieldsList[1].Name);
    }

    [Fact]
    public void ReadSchema_works_for_the_SelfContained_schema_blob()
    {
        var schema = SampleSchema();
        var emptyBatch = SampleBatch(schema, [], []);
        var schemaBlob = WriteFullStream(schema, [emptyBatch]);
        var decoder = new BqArrowStreamDecoder(schemaBlob);

        var read = decoder.ReadSchema();

        Assert.Equal(2, read.FieldsList.Count);
        Assert.Equal("id", read.FieldsList[0].Name);
        Assert.Equal("name", read.FieldsList[1].Name);
    }

    [Fact]
    public async Task Empty_blob_sequence_yields_no_batches()
    {
        var schema = SampleSchema();
        var schemaBytes = WriteSchemaOnly(schema);
        var decoder = new BqArrowStreamDecoder(schemaBytes);

        var batches = await CollectAsync(decoder.DecodeAsync(ToAsyncEnumerable([]), CancellationToken.None));

        Assert.Empty(batches);
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> CancelAfterFirstBlob(
        byte[] firstBlob, CancellationTokenSource cts, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        yield return firstBlob;
        cts.Cancel();
        ct.ThrowIfCancellationRequested();
        yield return firstBlob;
    }

    [Fact]
    public async Task Cancellation_between_blobs_throws_OperationCanceledException()
    {
        var schema = SampleSchema();
        var schemaBlob = WriteFullStream(schema, [SampleBatch(schema, [], [])]);
        var firstBlob = WriteFullStream(schema, [SampleBatch(schema, [1], ["a"])]);

        var decoder = new BqArrowStreamDecoder(schemaBlob);
        var cts = new CancellationTokenSource();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await CollectAsync(decoder.DecodeAsync(CancelAfterFirstBlob(firstBlob, cts), cts.Token)));
    }

    // Cancels the token before the first blob is even yielded, without the enumerator itself
    // checking it -- so the decoder's own probe of that first blob (DecodeFirstBlobAsync's
    // self-contained attempt) is what observes the cancellation and throws, not MoveNextAsync.
    // That is the only way to drive execution through the "catch (Exception ex) when (ex is not
    // OperationCanceledException)" guard rather than around it.
    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> CancelDuringFirstBlobProbe(
        byte[] firstBlob, CancellationTokenSource cts)
    {
        cts.Cancel();
        await Task.Yield();
        yield return firstBlob;
    }

    [Fact]
    public async Task Cancellation_during_the_first_blobs_layout_probe_propagates_unwrapped_not_as_batch_only()
    {
        var schema = SampleSchema();
        var schemaBlob = WriteFullStream(schema, [SampleBatch(schema, [], [])]);
        var firstBlob = WriteFullStream(schema, [SampleBatch(schema, [1], ["a"])]);

        var decoder = new BqArrowStreamDecoder(schemaBlob);
        var cts = new CancellationTokenSource();

        var ex = await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await CollectAsync(decoder.DecodeAsync(CancelDuringFirstBlobProbe(firstBlob, cts), cts.Token)));

        // The exact-type assertion above already rules out PzConnectorException; spelled out here
        // because that reclassification is precisely what the catch-filter guard exists to prevent.
        Assert.IsNotType<PzConnectorException>(ex);

        // Neither Layout assignment in DecodeFirstBlobAsync ran: the guard let the cancellation
        // through before the catch block could reclassify it as "must be BatchOnly."
        Assert.Equal(BlobLayout.Unknown, decoder.Layout);
    }
}
