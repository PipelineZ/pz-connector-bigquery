using System.Globalization;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>Sink behavior the TestKit contract does not pin: target creation/schema checks, spool
/// rolling into multiple load jobs, abort cleanup, <c>staging_dataset</c> routing, the full §7.2 type
/// matrix round-tripping through a real load, and how this emulator classifies a job failure.
///
/// <para><b>Three confirmed emulator limitations</b> (goccy/bigquery-emulator 0.8.1), each proved by
/// direct probing against the live container rather than assumed:</para>
/// <list type="number">
/// <item>Its MERGE statement parser refuses any <c>using (...)</c> source that is not a single bare
/// table reference -- <c>"MERGE: source must be a single-table reference, got
/// *googlesql.ResolvedProjectScan"</c> -- which is exactly the shape <see cref="BqSql.Merge"/> always
/// builds (a windowed dedup subquery), matching real BigQuery's own documented MERGE syntax (a
/// subquery source is explicitly legal there). Every merge into an ALREADY-EXISTING target fails on
/// this emulator specifically; <see cref="Merge_into_an_existing_target_fails_with_the_emulators_own_job_error"/>
/// below proves the failure is classified correctly (transient, naming the job's purpose) rather than
/// working around it, and <see cref="BqSinkAcceptance"/> skips the two base-suite facts that need a
/// second commit against an existing merge target for the same reason. <see cref="BqSql.Merge"/>'s
/// own exact SQL shape stays covered by <c>BqSqlTests</c> (unit-level), and the dispatch to it for an
/// existing target by <c>BqWriteSessionTests</c> (FakeHandler-level, no live GoogleSQL execution
/// needed) -- merge into a MISSING target never hits this limitation, since that path is a plain
/// <see cref="BqSql.DedupedSelect"/> query-destination job, not a MERGE statement.</item>
/// <item>A query-destination job's <c>writeDisposition: WRITE_TRUNCATE</c> is not honored -- it
/// behaves exactly like <c>WRITE_APPEND</c> instead. <see cref="BqSinkAcceptance"/> skips the
/// base suite's own replace fact for the same reason; <c>BqWriteSessionTests</c> proves this
/// connector still SENDS <c>WRITE_TRUNCATE</c> for replace, which is all that is provable without a
/// live service that actually honors it.</item>
/// <item>A NEWLINE_DELIMITED_JSON load job does not base64-DECODE a BYTES column's JSON string value
/// on ingest -- it stores the string's own UTF-8 bytes verbatim, so a value written as the
/// (correct, per BigQuery's documented load contract) base64 text of some bytes round-trips as
/// something else entirely. <see cref="Every_type_in_the_matrix_round_trips_through_a_load"/> below
/// therefore excludes BYTES; <c>BqJsonRowWriterTests.Bytes_are_base64_encoded</c> already proves this
/// connector writes the correct (real-BigQuery-correct) encoding.</item>
/// </list></summary>
[Collection("bigquery")]
[Trait("Category", "Docker")]
public sealed class BqSinkBehaviorTests
{
    private static readonly Schema IdName = new(
        [new Field("id", Int64Type.Default, nullable: true), new Field("name", StringType.Default, nullable: true)], null);

    private readonly BqFixture _bq;

    public BqSinkBehaviorTests(BqFixture bq)
    {
        _bq = bq;
        DockerFacts.SkipUnlessDocker();
    }

    private async Task<ISink> OpenSinkAsync(string? stagingDataset = null, long spoolRollBytes = 64 * 1024 * 1024)
    {
        ISinkConnector connector = new BqConnector(null, TimeProvider.System, spoolRollBytes);
        return await connector.OpenAsync(new ConnectorConfig(_bq.ConnectionConfig(stagingDataset)), CancellationToken.None);
    }

    private static OutputSpec Spec(string table, string mode = "append", string schemaPolicy = "fail_on_change", string[]? keys = null) =>
        new("bigquery", $"{BqFixture.Dataset}.{table}", mode, schemaPolicy, new Dictionary<string, object?>())
        {
            Keys = keys ?? [],
        };

    private static RecordBatch IdNameBatch(IEnumerable<(long? Id, string? Name)> rows)
    {
        var builder = new ArrowBatchBuilder(IdName);
        foreach (var (id, name) in rows)
        {
            builder.AppendRow([(object?)id, name]);
        }

        return builder.Flush()!;
    }

    private static async Task<WriteResult> WriteAsync(
        ISink sink, OutputSpec spec, Schema schema, IReadOnlyList<RecordBatch> batches)
    {
        await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        foreach (var batch in batches)
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
            batch.Dispose();
        }

        return await session.CommitAsync(CancellationToken.None);
    }

    private async Task<List<JsonElement>> SelectIdNameAsync(string table) =>
        await _bq.QueryAsync($"select id, name from {BqFixture.Dataset}.{table}");

    [SkippableFact]
    public async Task Append_creates_a_missing_target_then_appends_across_two_commits()
    {
        var table = BqFixture.NewName("append");
        var sink = await OpenSinkAsync();
        var spec = Spec(table);

        await WriteAsync(sink, spec, IdName, [IdNameBatch([(1, "a"), (2, "b"), (3, "c")])]);
        var result = await WriteAsync(sink, spec, IdName, [IdNameBatch([(4, "d"), (5, "e"), (6, "f")])]);

        Assert.Equal(3, result.RowsWritten);
        var rows = await SelectIdNameAsync(table);
        Assert.Equal(6, rows.Count);
    }

    // Not "Replace_leaves_exactly_the_second_commits_rows": this emulator does not honor
    // WRITE_TRUNCATE (see the class doc), so a genuine overwrite proof is unavailable here --
    // BqWriteSessionTests proves WRITE_TRUNCATE is what gets sent instead. This fact only proves
    // replace's CREATE path lands the written rows into a fresh target.
    [SkippableFact]
    public async Task Replace_creates_the_target_with_the_written_rows()
    {
        var table = BqFixture.NewName("replace");
        var sink = await OpenSinkAsync();
        var spec = Spec(table, mode: "replace");

        var result = await WriteAsync(sink, spec, IdName, [IdNameBatch(Enumerable.Range(0, 10).Select(i => ((long?)i, (string?)$"r{i}")))]);

        Assert.Equal(10, result.RowsWritten);
        var rows = await SelectIdNameAsync(table);
        Assert.Equal(10, rows.Count);
    }

    [SkippableFact]
    public async Task Merge_into_a_missing_target_creates_it_deduplicated_last_occurrence_wins()
    {
        var table = BqFixture.NewName("mergenew");
        var sink = await OpenSinkAsync();
        var spec = Spec(table, mode: "merge", keys: ["id"]);

        // id 0 appears twice within the SAME session (two batches); the later value must win even
        // though the target never existed before this commit.
        await WriteAsync(sink, spec, IdName,
            [IdNameBatch([(0, "first")]), IdNameBatch([(0, "second"), (1, "new")])]);

        var rows = await SelectIdNameAsync(table);
        Assert.Equal(2, rows.Count);
        var byId = rows.ToDictionary(r => r.GetProperty("id").GetString()!, r => r.GetProperty("name").GetString());
        Assert.Equal("second", byId["0"]);
        Assert.Equal("new", byId["1"]);
    }

    [SkippableFact]
    public async Task Merge_with_a_key_only_schema_into_a_missing_target_does_not_error()
    {
        var table = BqFixture.NewName("mergekeyonly");
        var idOnly = new Schema([new Field("id", Int64Type.Default, nullable: true)], null);
        var builder = new ArrowBatchBuilder(idOnly);
        builder.AppendRow([1L]);
        builder.AppendRow([1L]);
        builder.AppendRow([2L]);
        using var batch = builder.Flush()!;

        var sink = await OpenSinkAsync();
        var spec = Spec(table, mode: "merge", keys: ["id"]);
        var result = await WriteAsync(sink, spec, idOnly, [batch]);

        Assert.Equal(3, result.RowsWritten);
        var rows = await _bq.QueryAsync($"select id from {BqFixture.Dataset}.{table}");
        Assert.Equal(2, rows.Count);
    }

    [SkippableFact]
    public async Task Merge_into_an_existing_target_fails_with_the_emulators_own_job_error()
    {
        var table = BqFixture.NewName("mergeexisting");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]""");
        await _bq.InsertRowsAsync(table, ["""{"id":1,"name":"orig"}"""]);

        var sink = await OpenSinkAsync();
        var spec = Spec(table, mode: "merge", keys: ["id"]);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await WriteAsync(sink, spec, IdName, [IdNameBatch([(1, "changed")])]));

        Assert.True(ex.IsTransient);
        Assert.Contains("PZBQ0402", ex.Message);
        Assert.Contains("commit statement", ex.Message);

        // The failed job's own finally still ran: no staging table survives the failure.
        var leaked = (await _bq.ListTablesAsync(BqFixture.Dataset)).Where(t => t.StartsWith("pz_load_", StringComparison.Ordinal));
        Assert.Empty(leaked);
    }

    [SkippableFact]
    public async Task FailOnChange_mismatch_lists_the_offending_column()
    {
        var table = BqFixture.NewName("mismatch");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"},{"name":"name","type":"INTEGER"}]""");

        var sink = await OpenSinkAsync();
        var spec = Spec(table);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await WriteAsync(sink, spec, IdName, [IdNameBatch([(1, "a")])]));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0304", ex.Message);
        Assert.Contains("'name'", ex.Message);
    }

    [SkippableFact]
    public async Task Evolve_is_refused_before_any_table_exists()
    {
        var table = BqFixture.NewName("evolve");
        var sink = await OpenSinkAsync();
        var spec = Spec(table, schemaPolicy: "evolve");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await WriteAsync(sink, spec, IdName, [IdNameBatch([(1, "a")])]));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0305", ex.Message);
        Assert.False(await _bq.TableExistsAsync(table));
    }

    [SkippableFact]
    public async Task Spool_rolling_lands_every_row_across_multiple_load_jobs()
    {
        var table = BqFixture.NewName("rolling");
        var sink = await OpenSinkAsync(spoolRollBytes: 1);
        var spec = Spec(table);

        var batches = Enumerable.Range(0, 3)
            .Select(b => IdNameBatch(Enumerable.Range(b * 5, 5).Select(i => ((long?)i, (string?)$"r{i}"))))
            .ToList();
        var result = await WriteAsync(sink, spec, IdName, batches);

        Assert.Equal(15, result.RowsWritten);
        var rows = await SelectIdNameAsync(table);
        Assert.Equal(15, rows.Count);
        Assert.Equal(Enumerable.Range(0, 15).Select(i => i.ToString(CultureInfo.InvariantCulture)).OrderBy(s => s),
            rows.Select(r => r.GetProperty("id").GetString()).OrderBy(s => s));
    }

    [SkippableFact]
    public async Task Abort_after_a_write_leaves_no_staging_table_and_target_untouched()
    {
        var table = BqFixture.NewName("abort");
        var sink = await OpenSinkAsync();
        var spec = Spec(table);

        await using (var session = await sink.BeginWriteAsync(spec, IdName, CancellationToken.None))
        {
            using var batch = IdNameBatch([(1, "a")]);
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.AbortAsync(CancellationToken.None);
        }

        Assert.False(await _bq.TableExistsAsync(table));
        var leaked = (await _bq.ListTablesAsync(BqFixture.Dataset)).Where(t => t.StartsWith("pz_load_", StringComparison.Ordinal));
        Assert.Empty(leaked);
    }

    [SkippableFact]
    public async Task StagingDataset_option_lands_writes_correctly_and_leaves_nothing_behind()
    {
        var stagingDataset = BqFixture.NewName("staging");
        await _bq.CreateDatasetAsync(stagingDataset);
        var table = BqFixture.NewName("stagingok");

        var sink = await OpenSinkAsync(stagingDataset: stagingDataset);
        var spec = Spec(table);
        var result = await WriteAsync(sink, spec, IdName, [IdNameBatch([(1, "a"), (2, "b")])]);

        Assert.Equal(2, result.RowsWritten);
        var rows = await SelectIdNameAsync(table);
        Assert.Equal(2, rows.Count);

        var stagingTables = (await _bq.ListTablesAsync(stagingDataset)).Where(t => t.StartsWith("pz_load_", StringComparison.Ordinal));
        Assert.Empty(stagingTables);
    }

    [SkippableFact]
    public async Task StagingDataset_that_does_not_exist_surfaces_in_the_failure()
    {
        var missingDataset = BqFixture.NewName("missingds");
        var table = BqFixture.NewName("stagingmissing");

        var sink = await OpenSinkAsync(stagingDataset: missingDataset);
        var spec = Spec(table);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await WriteAsync(sink, spec, IdName, [IdNameBatch([(1, "a")])]));

        Assert.Contains(missingDataset, ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task List_column_is_refused_at_BeginWriteAsync()
    {
        var schema = new Schema(
            [new Field("id", Int64Type.Default, nullable: true), new Field("tags", new ListType(StringType.Default), nullable: true)], null);
        var sink = await OpenSinkAsync();
        var spec = Spec(BqFixture.NewName("listcol"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, schema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZBQ0303", ex.Message);
        Assert.Contains("'tags'", ex.Message);
    }

    [SkippableFact]
    public async Task Every_type_in_the_matrix_round_trips_through_a_load()
    {
        var utc = new TimestampType(TimeUnit.Microsecond, "UTC");
        var noTz = new TimestampType(TimeUnit.Microsecond, timezone: (string?)null);
        var time = new Time64Type(TimeUnit.Microsecond);
        var numeric = new Decimal128Type(38, 9);
        var bigNumeric = new Decimal128Type(38, 10);

        var schema = new Schema(
        [
            new Field("i", Int64Type.Default, true),
            new Field("f", DoubleType.Default, true),
            new Field("num", numeric, true),
            new Field("bignum", bigNumeric, true),
            new Field("s", StringType.Default, true),
            new Field("flag", BooleanType.Default, true),
            new Field("d", Date32Type.Default, true),
            new Field("dt", noTz, true),
            new Field("ts", utc, true),
            new Field("t", time, true),
        ], null);

        // Built as a DateTimeOffset with an explicit zero offset, not a bare DateTime -- a
        // DateTime's implicit conversion to DateTimeOffset applies the HOST's local timezone to an
        // Unspecified Kind, which would silently shift these wall-clock digits depending on where
        // this test happens to run.
        var dateTimeValue = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(60);
        var timestampValue = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(60);
        var timeValue = new TimeOnly(3, 4, 5).Add(TimeSpan.FromTicks(60));

        var arrays = new IArrowArray[]
        {
            new Int64Array.Builder().Append(42).Build(),
            new DoubleArray.Builder().Append(2.5).Build(),
            new Decimal128Array.Builder(numeric).Append(12.345m).Build(),
            new Decimal128Array.Builder(bigNumeric).Append(123.4567891234m).Build(),
            new StringArray.Builder().Append("hello world").Build(),
            new BooleanArray.Builder().Append(true).Build(),
            new Date32Array.Builder().Append(new DateTime(2026, 1, 2)).Build(),
            new TimestampArray.Builder(noTz).Append(dateTimeValue).Build(),
            new TimestampArray.Builder(utc).Append(timestampValue).Build(),
            new Time64Array.Builder(time).Append(timeValue).Build(),
        };
        var batch = new RecordBatch(schema, arrays, 1);

        var table = BqFixture.NewName("types");
        var sink = await OpenSinkAsync();
        var spec = Spec(table);
        await WriteAsync(sink, spec, schema, [batch]);

        var rows = await _bq.QueryAsync($"select * from {BqFixture.Dataset}.{table}");
        var row = Assert.Single(rows);

        Assert.Equal("42", row.GetProperty("i").GetString());
        Assert.Equal(2.5, double.Parse(row.GetProperty("f").GetString()!, CultureInfo.InvariantCulture));
        Assert.Equal(12.345m, decimal.Parse(row.GetProperty("num").GetString()!, CultureInfo.InvariantCulture));
        Assert.Equal(123.4567891234m, decimal.Parse(row.GetProperty("bignum").GetString()!, CultureInfo.InvariantCulture));
        Assert.Equal("hello world", row.GetProperty("s").GetString());
        Assert.Equal("true", row.GetProperty("flag").GetString());
        Assert.Equal("2026-01-02", row.GetProperty("d").GetString());
        Assert.Equal("2026-01-02T03:04:05.000006", row.GetProperty("dt").GetString());
        Assert.Equal("03:04:05.000006", row.GetProperty("t").GetString());

        // TIMESTAMP comes back as fractional Unix-epoch seconds ("1767323045.000006"), not an
        // ISO-8601 string -- confirmed against the live emulator, not assumed.
        var expectedEpochSeconds = (timestampValue - DateTimeOffset.UnixEpoch).TotalSeconds;
        var actualEpochSeconds = double.Parse(row.GetProperty("ts").GetString()!, CultureInfo.InvariantCulture);
        Assert.Equal(expectedEpochSeconds, actualEpochSeconds, precision: 5);
    }
}
