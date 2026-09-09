namespace Pz.Connector.BigQuery.Tests;

public sealed class BqSpoolTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-bigquery-spool-tests-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_dir))
            {
                Directory.Delete(_dir, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best effort: a test failure that leaves the directory locked must not mask the
            // original assertion failure with a cleanup exception.
        }
    }

    [Fact]
    public void Current_lazily_creates_the_directory_and_the_first_part_file()
    {
        Assert.False(Directory.Exists(_dir));

        var spool = new BqSpool(_dir);
        _ = spool.Current;

        Assert.True(Directory.Exists(_dir));
        Assert.True(File.Exists(Path.Combine(_dir, "part-00000.ndjson")));
    }

    [Fact]
    public async Task CloseAsync_with_nothing_written_returns_an_empty_list()
    {
        var spool = new BqSpool(_dir);
        var files = await spool.CloseAsync();

        Assert.Empty(files);
    }

    [Fact]
    public async Task RollIfNeededAsync_rolls_to_a_new_file_once_the_threshold_is_exceeded()
    {
        var spool = new BqSpool(_dir, rollBytes: 1);

        var writer = new StreamWriter(spool.Current, leaveOpen: true);
        await writer.WriteAsync("{\"a\":1}\n");
        await writer.FlushAsync();
        await writer.DisposeAsync();
        await spool.RollIfNeededAsync();

        var writer2 = new StreamWriter(spool.Current, leaveOpen: true);
        await writer2.WriteAsync("{\"a\":2}\n");
        await writer2.FlushAsync();
        await writer2.DisposeAsync();
        await spool.RollIfNeededAsync();

        var files = await spool.CloseAsync();

        Assert.Equal(2, files.Count);
        Assert.Equal(Path.Combine(_dir, "part-00000.ndjson"), files[0]);
        Assert.Equal(Path.Combine(_dir, "part-00001.ndjson"), files[1]);
        Assert.True(File.Exists(files[0]));
        Assert.True(File.Exists(files[1]));
    }

    [Fact]
    public async Task RollIfNeededAsync_does_not_roll_below_the_threshold()
    {
        var spool = new BqSpool(_dir, rollBytes: 1024 * 1024);

        var writer = new StreamWriter(spool.Current, leaveOpen: true);
        await writer.WriteAsync("{\"a\":1}\n");
        await writer.FlushAsync();
        await writer.DisposeAsync();
        await spool.RollIfNeededAsync();

        var files = await spool.CloseAsync();

        Assert.Single(files);
    }

    [Fact]
    public async Task CloseAsync_flushes_and_disposes_the_current_stream()
    {
        var spool = new BqSpool(_dir);
        var current = spool.Current;
        var writer = new StreamWriter(current, leaveOpen: true);
        await writer.WriteAsync("{\"a\":1}\n");
        await writer.FlushAsync();
        await writer.DisposeAsync();

        var files = await spool.CloseAsync();

        Assert.Single(files);
        Assert.Equal("{\"a\":1}\n", await File.ReadAllTextAsync(files[0]));
    }

    [Fact]
    public async Task Delete_closes_an_open_written_to_stream_before_removing_the_directory()
    {
        var spool = new BqSpool(_dir);
        var writer = new StreamWriter(spool.Current, leaveOpen: true);
        await writer.WriteAsync("{\"a\":1}\n");
        await writer.FlushAsync();
        await writer.DisposeAsync();
        Assert.True(Directory.Exists(_dir));
        Assert.True(spool.IsOpen);

        // An open file handle inside the directory does not stop Directory.Delete on Linux, but
        // does on Windows -- Delete() must close it first so this succeeds on every OS.
        spool.Delete();

        Assert.False(Directory.Exists(_dir));
        Assert.False(spool.IsOpen);

        // A second Delete(), with the directory already gone and no stream left open, is a no-op.
        var ex = Record.Exception(spool.Delete);
        Assert.Null(ex);
    }

    [Fact]
    public void Delete_tolerates_a_directory_that_was_never_created()
    {
        var spool = new BqSpool(_dir);

        var ex = Record.Exception(spool.Delete);

        Assert.Null(ex);
    }
}
