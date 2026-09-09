using System.Globalization;
using System.Text;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>TestKit source contract against the emulator. <c>small</c> is 120 rows (<c>id INT64</c>
/// 0..119, <c>name STRING</c> ~90 chars) read with a single stream -- the determinism fact reads its
/// first column twice and compares, so the stream/partition count must be identical across both reads.
/// <c>large</c> is 150 000 rows (via <see cref="BqFixture.LoadCsvAsync"/>) so mid-read cancellation is
/// observable. <c>window</c> seeds ids 0..10. All three tables are created once per fixture on first
/// use.
///
/// <para>Every dataset here pins <c>streams: 1</c>: this emulator's Storage Read API
/// (<see cref="BqFixture.Image"/> 0.8.1) refuses any <c>CreateReadSession</c> whose requested stream
/// count is not exactly 1 (<c>Unknown: currently supported only one stream</c>). The base suite's own
/// partition-union proof only ever calls <see cref="GetSpecWithPartitionOverride"/> with <c>1</c>, so
/// pinning every dataset to one stream keeps the whole suite meaningful against this emulator without
/// weakening what it proves.</para></summary>
[Collection("bigquery")]
[Trait("Category", "Docker")]
public sealed class BqSourceAcceptance : SourceConnectorAcceptanceTests
{
    private static readonly SemaphoreSlim Seed = new(1, 1);
    private static string? _small;
    private static string? _large;
    private static string? _window;
    private readonly BqFixture _bq;

    public BqSourceAcceptance(BqFixture bq)
    {
        _bq = bq;
        DockerFacts.SkipUnlessDocker();
        SeedAsync().GetAwaiter().GetResult();
    }

    protected override ConnectorConfig ValidConfig => new(_bq.ConnectionConfig());

    protected override DatasetSpec SmallDataset => new("bigquery", $"{BqFixture.Dataset}.{_small}",
        new Dictionary<string, object?> { ["streams"] = 1 });

    protected override DatasetSpec? LargeDataset => new("bigquery", $"{BqFixture.Dataset}.{_large}",
        new Dictionary<string, object?> { ["streams"] = 1 });

    protected override DatasetSpec? BoundedWindowDataset =>
        new DatasetSpec("bigquery", $"{BqFixture.Dataset}.{_window}", new Dictionary<string, object?> { ["streams"] = 1 })
        {
            WatermarkCursor = "id", WatermarkValue = "3", WatermarkUpperBound = "7",
        };

    protected override DatasetSpec? GetSpecWithPartitionOverride(int partitions) => SmallDataset with
    {
        Options = new Dictionary<string, object?>(SmallDataset.Options) { ["streams"] = partitions },
    };

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISourceConnector CreateSource() => new BqConnector();

    private async Task SeedAsync()
    {
        await Seed.WaitAsync();
        try
        {
            _small ??= await SeedSmallAsync();
            _large ??= await SeedLargeAsync();
            _window ??= await SeedWindowAsync();
        }
        finally
        {
            Seed.Release();
        }
    }

    private async Task<string> SeedSmallAsync()
    {
        var table = BqFixture.NewName("small");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]""");
        var name = new string('a', 90);
        await _bq.InsertRowsAsync(table, Enumerable.Range(0, 120).Select(i => $$"""{"id":{{i}},"name":"{{name}}"}"""));
        return table;
    }

    private async Task<string> SeedLargeAsync()
    {
        var table = BqFixture.NewName("large");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");

        var csv = new StringBuilder();
        csv.Append("throwaway\n");
        for (var i = 0; i < 150_000; i++)
        {
            csv.Append(i.ToString(CultureInfo.InvariantCulture)).Append('\n');
        }

        await _bq.LoadCsvAsync(table, csv.ToString());
        return table;
    }

    private async Task<string> SeedWindowAsync()
    {
        var table = BqFixture.NewName("window");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");
        await _bq.InsertRowsAsync(table, Enumerable.Range(0, 11).Select(i => $$"""{"id":{{i}}}"""));
        return table;
    }
}
