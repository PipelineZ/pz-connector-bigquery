namespace Pz.Connector.BigQuery;

/// <summary>The on-disk NDJSON spool a write session appends to before any network call: one file
/// per roll, opened lazily so a session that never calls <see cref="Current"/> never touches disk at
/// all -- an empty write still runs every downstream step (see <c>CommitAsync</c>'s zero-spool-files
/// note), so creating a directory nothing is written to would be a needless side effect.
///
/// <para>Not sealed: <see cref="Delete"/> is <see langword="virtual"/> purely so a test can force a
/// deterministic cleanup failure (a real directory-delete failure needs either a Windows-only open
/// handle or filesystem permissions a container commonly runs past as root) -- production code
/// never subclasses this.</para></summary>
internal class BqSpool(string dir, long rollBytes = 64 * 1024 * 1024)
{
    private const int BufferSize = 64 * 1024;

    private readonly List<string> _closedFiles = [];
    private FileStream? _current;
    private int _index;

    /// <summary>For diagnostics only (a cleanup-failure log line naming which directory could not
    /// be removed) -- never parsed or reused to reopen anything.</summary>
    public string Dir => dir;

    /// <summary>Whether a part file is currently open. For tests only: proves <see cref="CloseAsync"/>
    /// actually closed the handle rather than trusting a directory-delete's success, which does not
    /// distinguish an open handle from a closed one on every platform (Linux permits deleting a
    /// directory containing an open file; Windows does not).</summary>
    public bool IsOpen => _current is not null;

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
    /// which particular file failed to delete. This method itself may still throw on a genuine I/O
    /// failure -- every caller in <c>BqWriteSession</c> guards it individually rather than this type
    /// swallowing it silently, so a caller can log with the context (output name, commit vs. abort)
    /// this type does not have.</summary>
    public virtual void Delete()
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
        // FileOptions.Asynchronous only pays for itself on the FlushAsync/DisposeAsync calls this
        // type itself makes -- BqJsonRowWriter.Write, the only thing that ever writes to Current,
        // writes synchronously (see its own doc: the caller's batch must be fully consumed before
        // the call returns), so every row pays the async-handle overhead without using it.
        return new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize, FileOptions.Asynchronous);
    }
}
