using System.Runtime.CompilerServices;
using Apache.Arrow;
using Grpc.Core;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>One Storage Read API stream, read as Arrow batches. A fresh <see cref="BqArrowStreamDecoder"/>
/// is built for every <see cref="ReadAsync"/> call rather than shared across calls: the decoder learns
/// and remembers a blob layout the first time it sees one (see its own doc comment), and a partition
/// the engine reads more than once must not carry that memory forward from a previous, unrelated read.
///
/// <para>A batch's <see cref="RecordBatch"/> array buffers may alias the network-received memory the
/// decoder was handed for that blob (see <see cref="BqArrowStreamDecoder.DecodeAsync"/>) -- the
/// <c>ByteString.Memory</c> a <see cref="ReadRowsResponse"/> carries is exactly the kind of blob that
/// contract allows: it is never reused or overwritten once handed to the decoder, so it stays valid for
/// as long as the batches decoded from it are alive.</para></summary>
internal sealed class BqPartition(ReadOnlyMemory<byte> serializedSchema, string streamName, BqReadSessionFactory factory,
    BqRedactor redactor, TableRef table) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var decoder = new BqArrowStreamDecoder(serializedSchema);
        var context = $"reading {table.Dataset}.{table.Table}";
        var batches = decoder.DecodeAsync(factory.ReadRowsAsync(streamName, ct), ct);

        // A yield inside a try/catch is illegal in C#, so MoveNextAsync (where a mid-stream RpcException
        // actually surfaces) is isolated in its own try/catch and the yield happens outside it -- the
        // standard shape for classifying failures from a manually-driven IAsyncEnumerator.
        await using var enumerator = batches.GetAsyncEnumerator(ct);
        while (true)
        {
            bool hasNext;
            try
            {
                hasNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
            }
            // The gRPC client reports a LOCALLY cancelled call as StatusCode.Cancelled, not as an
            // OperationCanceledException -- classifying it as a remote failure here would turn the
            // engine's own cancellation into a reported connector error instead of the cooperative
            // stop the caller asked for, so it is rethrown as the cancellation it actually is.
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && ct.IsCancellationRequested)
            {
                throw new OperationCanceledException(ex.Message, ex, ct);
            }
            catch (RpcException ex)
            {
                throw BqErrors.FromRpc(ex, redactor, context);
            }

            if (!hasNext)
            {
                yield break;
            }

            yield return enumerator.Current;
        }
    }
}
