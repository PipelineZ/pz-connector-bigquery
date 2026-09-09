using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;
using Grpc.Core;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>A read session's schema bytes (IPC-serialized, decodable by
/// <see cref="BqArrowStreamDecoder"/>) plus the stream names the Storage Read API assigned it --
/// each stream is an independently readable partition of the same table read.</summary>
internal sealed record BqSessionInfo(ReadOnlyMemory<byte> SerializedSchema, IReadOnlyList<string> StreamNames);

/// <summary>Owns the Storage Read API gRPC client and the two calls this connector makes against
/// it: creating a session (one call per <see cref="ISource.PlanReadAsync"/>/<see cref="ISource.GetSchemaAsync"/>
/// resolution) and streaming one of its streams' rows. The client is built lazily -- constructing
/// one dials nothing by itself, but doing it eagerly in the constructor would mean every
/// <see cref="BqSource"/> open pays gRPC channel setup even for a run that never actually reads.</summary>
internal sealed class BqReadSessionFactory(BqConnectionConfig cfg, GoogleCredential? cred, BqRedactor r)
{
    private readonly SemaphoreSlim _clientLock = new(1, 1);
    private BigQueryReadClient? _client;

    public async Task<BigQueryReadClient> ClientAsync(CancellationToken ct)
    {
        if (_client is { } existing)
        {
            return existing;
        }

        await _clientLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_client is null)
            {
                var builder = new BigQueryReadClientBuilder();
                if (cfg.StorageEndpoint is not null)
                {
                    builder.Endpoint = cfg.StorageEndpoint;
                }

                // Only set for the emulator: real BigQuery is always TLS, and setting Insecure
                // against it would fail the handshake outright rather than degrade gracefully.
                if (cfg.PlaintextStorage)
                {
                    builder.ChannelCredentials = ChannelCredentials.Insecure;
                }

                if (cred is not null)
                {
                    builder.Credential = cred;
                }

                _client = await builder.BuildAsync(ct).ConfigureAwait(false);
            }
        }
        finally
        {
            _clientLock.Release();
        }

        return _client;
    }

    /// <summary>Creates one read session over <paramref name="table"/>. <paramref name="maxStreams"/>
    /// of 0 lets the service pick a stream count on its own -- used for a schema-only resolution,
    /// where the caller reads nothing from any stream. A failure here is always the whole call
    /// failing before any row is read, so it is classified and thrown eagerly rather than left for a
    /// caller to discover mid-enumeration the way <see cref="ReadRowsAsync"/>'s failures are.</summary>
    public async Task<BqSessionInfo> CreateAsync(
        TableRef table, IReadOnlyList<string>? columns, string? restriction, int maxStreams, CancellationToken ct)
    {
        var client = await ClientAsync(ct).ConfigureAwait(false);

        var readOptions = new ReadSession.Types.TableReadOptions();
        if (columns is { Count: > 0 })
        {
            readOptions.SelectedFields.AddRange(columns);
        }

        if (!string.IsNullOrEmpty(restriction))
        {
            readOptions.RowRestriction = restriction;
        }

        var session = new ReadSession
        {
            Table = table.ResourcePath,
            DataFormat = DataFormat.Arrow,
            ReadOptions = readOptions,
        };

        var context = $"creating a read session for {table.Dataset}.{table.Table}";
        try
        {
            var created = await client.CreateReadSessionAsync($"projects/{cfg.Project}", session, maxStreams, ct).ConfigureAwait(false);
            return new BqSessionInfo(created.ArrowSchema.SerializedSchema.Memory, created.Streams.Select(s => s.Name).ToList());
        }
        // See BqPartition.ReadAsync's identical guard: a locally cancelled call surfaces as
        // StatusCode.Cancelled, not as an OperationCanceledException.
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && ct.IsCancellationRequested)
        {
            throw new OperationCanceledException(ex.Message, ex, ct);
        }
        catch (RpcException ex)
        {
            throw BqErrors.FromRpc(ex, r, context);
        }
    }

    /// <summary>Streams one read stream's Arrow IPC blobs, unclassified: a failure here happens mid
    /// enumeration, after some batches may already have reached the caller, so only the caller (which
    /// knows whether it already yielded anything) is positioned to decide how to report it --
    /// <see cref="BqPartition.ReadAsync"/> is that caller. A response carrying no
    /// <see cref="ReadRowsResponse.ArrowRecordBatch"/> (a throttle-only heartbeat, say) is skipped
    /// rather than yielded as an empty blob.</summary>
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReadRowsAsync(
        string stream, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var client = await ClientAsync(ct).ConfigureAwait(false);
        using var call = client.ReadRows(new ReadRowsRequest { ReadStream = stream });
        await foreach (var response in call.GetResponseStream().WithCancellation(ct).ConfigureAwait(false))
        {
            if (response.ArrowRecordBatch is { } batch)
            {
                yield return batch.SerializedRecordBatch.Memory;
            }
        }
    }
}
