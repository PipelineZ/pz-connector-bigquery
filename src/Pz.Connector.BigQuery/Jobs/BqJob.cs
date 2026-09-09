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
        return await WaitAsync(rest, project, inserted, job.JobReference?.Location, purpose, time, logger, ct).ConfigureAwait(false);
    }

    /// <summary>Polls an already-submitted job to <c>DONE</c>, applying the same backoff schedule as
    /// <see cref="SubmitAndWaitAsync"/> -- used by the load path, whose job is submitted through the
    /// resumable-upload handshake (<see cref="BqRestClient.UploadLoadJobAsync"/>) rather than
    /// <c>jobs.insert</c>, so there is no separate insert call here to make. <paramref name="submitted"/>
    /// is whatever that submission call already returned (a <c>jobs.insert</c> or upload-PUT
    /// response), which may itself already report <c>DONE</c> -- exactly the case
    /// <see cref="SubmitAndWaitAsync"/>'s own doc describes, classified here with no extra
    /// <c>jobs.get</c> call either.</summary>
    public static async Task<BqJob> WaitAsync(
        BqRestClient rest, string project, BqJob submitted, string? fallbackLocation, string purpose,
        TimeProvider time, ILogger logger, CancellationToken ct)
    {
        var jobId = submitted.JobReference?.JobId
            ?? throw new InvalidOperationException("the submitted job carries no jobReference.jobId to poll");
        // The submission response usually echoes jobReference.location back, but is not guaranteed
        // to -- falling back to the caller-supplied location keeps every jobs.get call addressed to
        // the same region the job actually runs in.
        var location = submitted.JobReference?.Location ?? fallbackLocation;

        var current = submitted;
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
