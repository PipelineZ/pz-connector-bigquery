using System.Globalization;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.BigQuery;

/// <summary>Maps a finished job's <c>statistics</c> onto the stats contract. Telemetry never fails an
/// operation: absent or unparseable values report nothing.</summary>
internal static class BqTelemetry
{
    public static void ReportJob(BqJobStatistics? statistics, IConnectorStats stats)
    {
        if (statistics is null)
        {
            return;
        }

        if (ParseInt64(statistics.Query?.TotalBytesBilled) is > 0 and var billed)
        {
            stats.AddCost(billed, "bytes_billed");
        }

        if (ParseInt64(statistics.StartTime) is { } start && ParseInt64(statistics.EndTime) is { } end && end > start)
        {
            stats.AddRemoteMs(end - start);
        }

        if (ParseInt64(statistics.TotalSlotMs) is { } slotMs)
        {
            stats.AddExtra("bigquery.slot_ms", slotMs);
        }

        if (ParseInt64(statistics.TotalBytesProcessed ?? statistics.Query?.TotalBytesProcessed) is { } processed)
        {
            stats.AddExtra("bigquery.bytes_processed", processed);
        }
    }

    private static long? ParseInt64(string? value) =>
        long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var n) ? n : null;
}
