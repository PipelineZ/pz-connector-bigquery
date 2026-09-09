using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Pz.Connector.BigQuery;

/// <summary>How a Storage Read API response blob is framed. Real BigQuery sends a
/// <c>serialized_schema</c> that is a lone IPC Schema message and a <c>serialized_record_batch</c>
/// that is a lone RecordBatch message -- readable only once the schema bytes are prepended. Local
/// emulators instead frame every blob, schema and batch alike, as a complete IPC stream (Schema +
/// data + EOS) that decodes on its own. The two shapes are indistinguishable from the session's own
/// metadata, so the decoder learns which one it is handed from the first blob and keeps that answer:
/// probing every blob would risk a coincidental prefix reading as the wrong layout mid-stream.</summary>
internal enum BlobLayout { Unknown, SelfContained, BatchOnly }

/// <summary>Decodes Storage Read API Arrow IPC blobs into <see cref="RecordBatch"/>es, coping with
/// both framings <see cref="BlobLayout"/> describes without configuration.</summary>
internal sealed class BqArrowStreamDecoder(ReadOnlyMemory<byte> serializedSchema)
{
    private readonly ReadOnlyMemory<byte> _serializedSchema = serializedSchema;

    /// <summary><see cref="BlobLayout.Unknown"/> until the first blob decides it.</summary>
    internal BlobLayout Layout { get; private set; } = BlobLayout.Unknown;

    /// <summary>Decodes the session's schema bytes alone. Works for both framings: the emulator's
    /// schema blob carries a trailing empty batch and EOS that this never reads past, since only the
    /// schema message is consumed to answer <c>reader.Schema</c>.</summary>
    public Schema ReadSchema()
    {
        using var reader = new ArrowStreamReader(_serializedSchema);
        return reader.Schema;
    }

    /// <summary>The <see cref="BlobLayout.SelfContained"/> path decodes each blob's memory
    /// zero-copy: a yielded batch's array buffers may be direct slices over the
    /// <see cref="ReadOnlyMemory{T}"/> the caller handed in for that blob, not a private copy. Each
    /// blob's memory must therefore stay valid and unmodified for as long as any batch decoded from
    /// it is still alive, and callers must never pass a rented or otherwise reused buffer as a
    /// blob -- reusing or overwriting it after yielding would silently corrupt already-yielded
    /// batches rather than fail loudly.</summary>
    public async IAsyncEnumerable<RecordBatch> DecodeAsync(
        IAsyncEnumerable<ReadOnlyMemory<byte>> serializedBatches,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var blob in serializedBatches.WithCancellation(ct))
        {
            var batches = Layout switch
            {
                BlobLayout.SelfContained => await DecodeSelfContainedAsync(blob, ct),
                BlobLayout.BatchOnly => await DecodeWithSchemaPrefixAsync(blob, ct),
                _ => await DecodeFirstBlobAsync(blob, ct),
            };

            foreach (var batch in batches)
            {
                yield return batch;
            }
        }
    }

    // The first blob decides the layout for the rest of the stream: a self-contained decode either
    // succeeds (the blob carries its own schema message) or throws (the blob is a lone RecordBatch
    // message with no schema first), and that outcome is trusted for every later blob rather than
    // re-probed -- a coincidental byte prefix in a later blob must never flip the decision.
    private async Task<List<RecordBatch>> DecodeFirstBlobAsync(ReadOnlyMemory<byte> blob, CancellationToken ct)
    {
        try
        {
            var batches = await DecodeSelfContainedAsync(blob, ct);
            Layout = BlobLayout.SelfContained;
            return batches;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Layout = BlobLayout.BatchOnly;
            return await DecodeWithSchemaPrefixAsync(blob, ct);
        }
    }

    private static async Task<List<RecordBatch>> DecodeSelfContainedAsync(ReadOnlyMemory<byte> blob, CancellationToken ct)
    {
        using var reader = new ArrowStreamReader(blob);
        return await ReadAllAsync(reader, ct);
    }

    // Real BigQuery's serialized_record_batch has no schema message of its own; concatenating the
    // session's serialized_schema in front makes it a well-formed one-batch IPC stream. Copying the
    // schema bytes per blob is deliberate: the schema is a few hundred bytes, and building a real
    // multi-source stream bridge would cost more than the copy it avoids.
    private async Task<List<RecordBatch>> DecodeWithSchemaPrefixAsync(ReadOnlyMemory<byte> blob, CancellationToken ct)
    {
        var combined = new byte[_serializedSchema.Length + blob.Length];
        _serializedSchema.Span.CopyTo(combined);
        blob.Span.CopyTo(combined.AsSpan(_serializedSchema.Length));

        using var reader = new ArrowStreamReader(combined.AsMemory());
        return await ReadAllAsync(reader, ct);
    }

    // An empty batch (Length == 0) means nothing: the emulator's schema blob carries one as part of
    // its self-contained framing, and an empty stream is expressed by yielding nothing at all.
    private static async Task<List<RecordBatch>> ReadAllAsync(ArrowStreamReader reader, CancellationToken ct)
    {
        var batches = new List<RecordBatch>();
        RecordBatch? batch;
        while ((batch = await reader.ReadNextRecordBatchAsync(ct)) is not null)
        {
            if (batch.Length == 0)
            {
                batch.Dispose();
                continue;
            }

            batches.Add(batch);
        }

        return batches;
    }
}
