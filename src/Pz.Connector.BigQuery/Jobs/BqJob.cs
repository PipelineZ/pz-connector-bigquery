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

    /// <summary>Inserts <paramref name="job"/> and waits for it to reach <c>DONE</c>, applying the
    /// backoff schedule 250 ms → 500 ms → 1 s → 2 s (then 2 s forever) between polls through
    /// <paramref name="time"/> so a test drives it with a <c>FakeTimeProvider</c> instead of a real
    /// sleep. An insert response already reporting <c>DONE</c> is itself a terminal status -- nothing
    /// requires a job to still be running just because it was only just submitted -- so it is
    /// classified directly with no <c>jobs.get</c> call at all; only a response reporting anything
    /// else falls through to polling, whose very first check is a fresh <c>jobs.get</c> issued with
    /// no delay, every later one separated by the schedule above.</summary>
    public static async Task<BqJob> SubmitAndWaitAsync(
        BqRestClient rest, string project, BqJob job, string purpose, TimeProvider time, ILogger logger, CancellationToken ct)
    {
        var inserted = await rest.InsertJobAsync(project, job, ct).ConfigureAwait(false);
        var jobId = inserted.JobReference?.JobId
            ?? throw new InvalidOperationException("the inserted job carries no jobReference.jobId to poll");
        // The insert response usually echoes jobReference.location back, but is not guaranteed to
        // -- falling back to the location the caller submitted keeps every jobs.get call addressed
        // to the same region the job actually runs in.
        var location = inserted.JobReference?.Location ?? job.JobReference?.Location;

        var current = inserted;
        if (!string.Equals(current.Status?.State, "DONE", StringComparison.Ordinal))
        {
            current = await rest.GetJobAsync(project, jobId, location, ct).ConfigureAwait(false);
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
        }

        if (current.Status?.ErrorResult is { } error)
        {
            throw BqErrors.FromJobError(error.Reason, error.Message, rest.Redactor, purpose);
        }

        logger.LogDebug("bigquery job {JobId} ({Purpose}) finished", jobId, purpose);
        return current;
    }
}
