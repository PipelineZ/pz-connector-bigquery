using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery.Tests;

/// <summary>Source behavior the TestKit contract does not pin: the declared Arrow schema against a
/// real BigQuery table, pushdown (columns/predicate/watermark), how the emulator handles an empty
/// table and a missing one, and the view-refusal path.</summary>
[Collection("bigquery")]
[Trait("Category", "Docker")]
public sealed class BqSourceBehaviorTests
{
    private readonly BqFixture _bq;

    public BqSourceBehaviorTests(BqFixture bq)
    {
        _bq = bq;
        DockerFacts.SkipUnlessDocker();
    }

    // Every spec defaults to a single stream: this emulator's Storage Read API
    // (BqFixture.Image, 0.8.1) refuses any CreateReadSession whose requested stream count is not
    // exactly 1 ("Unknown: currently supported only one stream") -- a caller that genuinely wants to
    // exercise a different count (Overriding_stream_count_is_rejected_by_this_emulator below) passes
    // it explicitly and it overrides this default.
    private static DatasetSpec Spec(string table, Dictionary<string, object?>? options = null)
    {
        var merged = options is null ? [] : new Dictionary<string, object?>(options);
        merged.TryAdd("streams", 1);
        return new DatasetSpec("bigquery", $"{BqFixture.Dataset}.{table}", merged);
    }

    private async Task<ISource> OpenAsync()
    {
        ISourceConnector connector = new BqConnector();
        return await connector.OpenAsync(new ConnectorConfig(_bq.ConnectionConfig()), CancellationToken.None);
    }

    private async Task<Schema> SchemaAsync(DatasetSpec spec)
    {
        await using var source = await OpenAsync();
        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        return schema.Schema;
    }

    private async Task<List<RecordBatch>> ReadAsync(DatasetSpec spec, ReadHints? hints = null)
    {
        await using var source = await OpenAsync();
        var partitions = await source.PlanReadAsync(spec, hints ?? ReadHints.None, CancellationToken.None);
        var batches = new List<RecordBatch>();
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batches.Add(batch);
            }
        }

        return batches;
    }

    private static Field FieldOf(Schema schema, string name) =>
        schema.GetFieldByName(name) ?? throw new InvalidOperationException($"schema has no field '{name}'");

    private static List<long> LongColumn(IReadOnlyList<RecordBatch> batches, string name)
    {
        var result = new List<long>();
        foreach (var batch in batches)
        {
            var i = batch.Schema.FieldsList.ToList().FindIndex(f => f.Name == name);
            var column = (Int64Array)batch.Column(i);
            for (var row = 0; row < batch.Length; row++)
            {
                result.Add(column.GetValue(row)!.Value);
            }
        }

        return result;
    }

    [SkippableFact]
    public async Task Schema_declares_every_supported_scalar_and_nested_type()
    {
        var table = BqFixture.NewName("types");
        await _bq.CreateTableAsync(table, """
            [
              {"name":"bool_col","type":"BOOLEAN"},
              {"name":"int_col","type":"INTEGER"},
              {"name":"float_col","type":"FLOAT"},
              {"name":"numeric_col","type":"NUMERIC"},
              {"name":"bignumeric_col","type":"BIGNUMERIC"},
              {"name":"string_col","type":"STRING"},
              {"name":"bytes_col","type":"BYTES"},
              {"name":"date_col","type":"DATE"},
              {"name":"datetime_col","type":"DATETIME"},
              {"name":"time_col","type":"TIME"},
              {"name":"timestamp_col","type":"TIMESTAMP"},
              {"name":"geo_col","type":"GEOGRAPHY"},
              {"name":"json_col","type":"JSON"},
              {"name":"array_col","type":"INTEGER","mode":"REPEATED"},
              {"name":"struct_col","type":"RECORD","fields":[{"name":"a","type":"INTEGER"}]}
            ]
            """);

        var schema = await SchemaAsync(Spec(table));

        Assert.Equal(ArrowTypeId.Boolean, FieldOf(schema, "bool_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.Int64, FieldOf(schema, "int_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.Double, FieldOf(schema, "float_col").DataType.TypeId);

        var numeric = Assert.IsType<Decimal128Type>(FieldOf(schema, "numeric_col").DataType);
        Assert.Equal(38, numeric.Precision);
        Assert.Equal(9, numeric.Scale);

        var bignumeric = Assert.IsType<Decimal256Type>(FieldOf(schema, "bignumeric_col").DataType);
        Assert.Equal(76, bignumeric.Precision);
        Assert.Equal(38, bignumeric.Scale);

        Assert.Equal(ArrowTypeId.String, FieldOf(schema, "string_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.Binary, FieldOf(schema, "bytes_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.Date32, FieldOf(schema, "date_col").DataType.TypeId);

        // Timezone is deliberately not asserted here: the emulator's own choice differs from
        // documented real-BigQuery behavior for these two types, and pinning it is a later, live-only
        // proof -- only the shape a caller needs to decode the column (kind, time unit) is checked.
        var datetime = Assert.IsType<TimestampType>(FieldOf(schema, "datetime_col").DataType);
        Assert.Equal(TimeUnit.Microsecond, datetime.Unit);

        var time = Assert.IsType<Time64Type>(FieldOf(schema, "time_col").DataType);
        Assert.Equal(TimeUnit.Microsecond, time.Unit);

        var timestamp = Assert.IsType<TimestampType>(FieldOf(schema, "timestamp_col").DataType);
        Assert.Equal(TimeUnit.Microsecond, timestamp.Unit);

        Assert.Equal(ArrowTypeId.String, FieldOf(schema, "geo_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.String, FieldOf(schema, "json_col").DataType.TypeId);

        var array = Assert.IsType<ListType>(FieldOf(schema, "array_col").DataType);
        Assert.Equal(ArrowTypeId.Int64, array.ValueDataType.TypeId);

        var @struct = Assert.IsType<StructType>(FieldOf(schema, "struct_col").DataType);
        Assert.Equal(["a"], @struct.Fields.Select(f => f.Name));
    }

    [SkippableFact]
    public async Task Column_hint_projects_a_single_column()
    {
        var table = BqFixture.NewName("proj");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]""");
        await _bq.InsertRowsAsync(table, ["""{"id":1,"name":"a"}""", """{"id":2,"name":"b"}"""]);

        var batches = await ReadAsync(Spec(table), new ReadHints(Columns: ["name"]));

        Assert.Equal(["name"], batches[0].Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(2, batches.Sum(b => b.Length));
    }

    [SkippableFact]
    public async Task Predicate_hint_filters_rows()
    {
        var table = BqFixture.NewName("pred");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");
        await _bq.InsertRowsAsync(table, Enumerable.Range(0, 120).Select(i => $$"""{"id":{{i}}}"""));

        var batches = await ReadAsync(Spec(table), new ReadHints(PredicateSql: "id >= 100"));

        Assert.Equal(20, batches.Sum(b => b.Length));
    }

    [SkippableFact]
    public async Task Int64_cursor_watermark_is_strict_inclusive_and_upper_bounded()
    {
        var table = BqFixture.NewName("cursor");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");
        await _bq.InsertRowsAsync(table, Enumerable.Range(0, 11).Select(i => $$"""{"id":{{i}}}"""));

        var strict = await ReadAsync(Spec(table) with { WatermarkCursor = "id", WatermarkValue = "3" });
        Assert.Equal([4, 5, 6, 7, 8, 9, 10], LongColumn(strict, "id").Order());

        var inclusive = await ReadAsync(Spec(table) with { WatermarkCursor = "id", WatermarkValue = "3", WatermarkLowerInclusive = true });
        Assert.Equal([3, 4, 5, 6, 7, 8, 9, 10], LongColumn(inclusive, "id").Order());

        var windowed = await ReadAsync(Spec(table) with { WatermarkCursor = "id", WatermarkValue = "3", WatermarkUpperBound = "7" });
        Assert.Equal([4, 5, 6, 7], LongColumn(windowed, "id").Order());
    }

    [SkippableFact]
    public async Task Timestamp_and_date_cursors_filter_rows()
    {
        var table = BqFixture.NewName("temporal");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"},{"name":"ts","type":"TIMESTAMP"},{"name":"d","type":"DATE"}]""");
        await _bq.InsertRowsAsync(table, Enumerable.Range(0, 10).Select(i =>
            $$"""{"id":{{i}},"ts":"2026-01-{{(i + 1).ToString("00")}}T00:00:00","d":"2026-01-{{(i + 1).ToString("00")}}"}"""));

        var byTimestamp = await ReadAsync(Spec(table) with { WatermarkCursor = "ts", WatermarkValue = "2026-01-01T00:00:00.000000" });
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9], LongColumn(byTimestamp, "id").Order());

        var byDate = await ReadAsync(Spec(table) with { WatermarkCursor = "d", WatermarkValue = "2026-01-05" });
        Assert.Equal([5, 6, 7, 8, 9], LongColumn(byDate, "id").Order());
    }

    [SkippableFact]
    public async Task Single_stream_read_covers_every_row_exactly_once()
    {
        var table = BqFixture.NewName("streams");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");
        await _bq.InsertRowsAsync(table, Enumerable.Range(0, 120).Select(i => $$"""{"id":{{i}}}"""));

        var batches = await ReadAsync(Spec(table));

        var ids = LongColumn(batches, "id");
        Assert.Equal(120, ids.Count);
        Assert.Equal(Enumerable.Range(0, 120).Select(i => (long)i), ids.Distinct().Order());
    }

    // This emulator's Storage Read API (BqFixture.Image, 0.8.1) only ever grants a single stream --
    // requesting more is not silently capped, it is refused outright ("Unknown: currently supported
    // only one stream"), so the multi-stream union invariant the connector's own PlanReadAsync/BqPartition
    // plumbing is built for cannot be exercised end-to-end against this image. What IS provable here is
    // that the 'streams' dataset option really reaches CreateReadSession unchanged, and that the
    // emulator's refusal comes back as a classified PzConnectorException (BqErrors.FromRpc treats gRPC
    // Unknown as transient -- a genuinely transient status in real BigQuery, even though this
    // particular cause of it never clears on retry) rather than an unclassified crash.
    [SkippableFact]
    public async Task Requesting_more_than_one_stream_reports_the_emulators_own_refusal()
    {
        var table = BqFixture.NewName("streams3");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");
        await _bq.InsertRowsAsync(table, ["""{"id":1}"""]);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => ReadAsync(Spec(table, new Dictionary<string, object?> { ["streams"] = 3 })));

        Assert.True(ex.IsTransient);
        Assert.Contains("currently supported only one stream", ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Empty_table_yields_zero_rows_and_a_resolvable_schema()
    {
        var table = BqFixture.NewName("empty");
        await _bq.CreateTableAsync(table, """[{"name":"id","type":"INTEGER"}]""");

        var schema = await SchemaAsync(Spec(table));
        Assert.Equal(["id"], schema.FieldsList.Select(f => f.Name));

        var batches = await ReadAsync(Spec(table));
        Assert.Equal(0, batches.Sum(b => b.Length));
    }

    // Real BigQuery's Storage Read API reports a missing table as gRPC NotFound, which
    // BqErrors.FromRpc classifies as PZBQ0205 -- that mapping is exercised by unit tests against a
    // synthetic RpcException. This emulator (BqFixture.Image, 0.8.1) instead reports it as gRPC
    // Unknown ("failed to get table metadata: table ... is not found in dataset ..."), which
    // BqErrors.FromRpc classifies as the generic transient PZBQ0408 -- so this fact proves the
    // emulator's actual failure reaches the caller as a classified PzConnectorException naming the
    // table, not that it takes the PZBQ0205 path a real NotFound status would.
    [SkippableFact]
    public async Task Missing_table_is_reported_as_a_classified_failure_naming_it()
    {
        var missing = BqFixture.NewName("missing");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => SchemaAsync(Spec(missing)));

        Assert.Contains(missing, ex.Message, StringComparison.Ordinal);
    }

    // Real BigQuery's Storage Read API refuses CreateReadSession over a view with gRPC
    // InvalidArgument, which BqSource.CreateSessionAsync catches and turns into PZBQ0204 (naming
    // `query:` as the next step) only after confirming via tables.get that the resource really is a
    // view -- that InvalidArgument-then-tables.get path is what a unit test against a canned
    // RpcException would exercise, but constructing one requires faking the Storage Read API's gRPC
    // transport, which is out of reach without a gRPC test server. Against THIS emulator
    // (BqFixture.Image, 0.8.1), CreateReadSession does not refuse a view at all -- it serves it like
    // any other readable table -- so the fact provable here is the emulator's actual behavior: a view
    // reads successfully and returns the same rows as its underlying table.
    [SkippableFact]
    public async Task A_view_is_read_successfully_by_this_emulator_unlike_real_BigQuery()
    {
        var baseTable = BqFixture.NewName("viewbase");
        await _bq.CreateTableAsync(baseTable, """[{"name":"id","type":"INTEGER"}]""");
        await _bq.InsertRowsAsync(baseTable, ["""{"id":1}""", """{"id":2}"""]);

        var view = BqFixture.NewName("view");
        await _bq.ExecuteStatementAsync(
            $"create view `{BqFixture.Project}`.{BqFixture.Dataset}.{view} as select * from {BqFixture.Dataset}.{baseTable}");

        var schema = await SchemaAsync(Spec(view));
        Assert.Equal(["id"], schema.FieldsList.Select(f => f.Name));

        var batches = await ReadAsync(Spec(view));
        Assert.Equal([1, 2], LongColumn(batches, "id").Order());
    }
}
