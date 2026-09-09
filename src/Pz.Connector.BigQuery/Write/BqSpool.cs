namespace Pz.Connector.BigQuery;

/// <summary>The on-disk NDJSON spool a write session appends to before any network call: one file
/// per roll, opened lazily so a session that never calls <see cref="Current"/> never touches disk at
/// all -- an empty write still runs every downstream step (see <c>CommitAsync</c>'s zero-spool-files
/// note), so creating a directory nothing is written to would be a needless side effect.</summary>
internal sealed class BqSpool(string dir, long rollBytes = 64 * 1024 * 1024)
{
    private const int BufferSize = 64 * 1024;

    private readonly List<string> _closedFiles = [];
    private FileStream? _current;
    private int _index;

    public Stream Current => _current ??= Open();

    /// <summary>Checked once per batch, after the batch has already been written -- a batch's rows
    /// therefore never split across two files, at the cost of a file occasionally running somewhat
    /// past <paramref name="rollBytes"/>.</summary>
    public async Task RollIfNeededAsync()
    {
        if (_current is null)
        {
            return;
        }

        await _current.FlushAsync().ConfigureAwait(false);
        if (_current.Length <= rollBytes)
        {
            return;
        }

        await CloseCurrentAsync().ConfigureAwait(false);
        _index++;
    }

    public async Task<IReadOnlyList<string>> CloseAsync()
    {
        await CloseCurrentAsync().ConfigureAwait(false);
        return _closedFiles;
    }

    /// <summary>Best effort by construction: a caller past this point (a killed process's next
    /// <c>pz retry</c>, an <c>AbortAsync</c>) only ever needs the directory gone, never a report of
    /// which particular file failed to delete.</summary>
    public void Delete()
    {
        if (Directory.Exists(dir))
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    private async Task CloseCurrentAsync()
    {
        if (_current is null)
        {
            return;
        }

        await _current.FlushAsync().ConfigureAwait(false);
        var path = _current.Name;
        await _current.DisposeAsync().ConfigureAwait(false);
        _current = null;
        _closedFiles.Add(path);
    }

    private FileStream Open()
    {
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, $"part-{_index:D5}.ndjson");
        return new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize, FileOptions.Asynchronous);
    }
}
