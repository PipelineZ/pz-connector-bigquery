using Microsoft.Extensions.Logging;

namespace Pz.Connector.BigQuery;

/// <summary>Submission and polling over the <see cref="BqJob"/> wire shape. A
/// client-generated <c>jobReference.jobId</c> is what makes a retried <c>jobs.insert</c> after a
/// lost response land on <c>409 duplicate</c> instead of a second real job -- <see cref="NewJobId"/>
/// mints that id, and <see cref="BqRestClient.InsertJobAsync"/> is what falls through to
/// <c>jobs.get</c> on the resulting 409.</summary>
internal sealed partial record BqJob
{
    private static readonly TimeSpan[] PollDelays =
    [
        TimeSpan.FromMilliseconds(250),
        TimeSpan.FromMilliseconds(500),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
    ];

    public static string NewJobId(string purpose) => $"pz_{purpose}_{Guid.NewGuid():N}";

    /// <summary>Inserts <paramref name="job"/> and polls <c>jobs.get</c> until <c>DONE</c>, applying
    /// the backoff schedule 250 ms → 500 ms → 1 s → 2 s (then 2 s forever) between calls through
    /// <paramref name="time"/> so a test drives it with a <c>FakeTimeProvider</c> instead of a real
    /// sleep. The insert response's own status is not trusted for completion -- BigQuery's
    /// <c>jobs.insert</c> answers as soon as the job is accepted, essentially never already
    /// <c>DONE</c> -- so the very first completion check is always a fresh <c>jobs.get</c>, issued
    /// with no delay, and every later one is separated by the schedule above.</summary>
    public static async Task<BqJob> SubmitAndWaitAsync(
        BqRestClient rest, string project, BqJob job, string purpose, TimeProvider time, ILogger logger, CancellationToken ct)
    {
        var inserted = await rest.InsertJobAsync(project, job, ct).ConfigureAwait(false);
        var jobId = inserted.JobReference?.JobId
            ?? throw new InvalidOperationException("the inserted job carries no jobReference.jobId to poll");
        var location = inserted.JobReference?.Location;

        var current = await rest.GetJobAsync(project, jobId, location, ct).ConfigureAwait(false);
        var delayIndex = 0;

        while (!string.Equals(current.Status?.State, "DONE", StringComparison.Ordinal))
        {
            var delay = PollDelays[Math.Min(delayIndex, PollDelays.Length - 1)];
            delayIndex++;
            logger.LogDebug("bigquery job {JobId} ({Purpose}) is {State}; waiting {Delay} before polling again",
                jobId, purpose, current.Status?.State, delay);
            await Task.Delay(delay, time, ct).ConfigureAwait(false);
            current = await rest.GetJobAsync(project, jobId, location, ct).ConfigureAwait(false);
        }

        if (current.Status?.ErrorResult is { } error)
        {
            throw BqErrors.FromJobError(error.Reason, error.Message, rest.Redactor, purpose);
        }

        logger.LogDebug("bigquery job {JobId} ({Purpose}) finished", jobId, purpose);
        return current;
    }
}
