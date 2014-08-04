package stork.scheduler;

import java.util.*;
import java.util.concurrent.*;

import stork.*;
import stork.ad.*;
import stork.util.*;
import stork.module.*;
import stork.feather.*;
import static stork.scheduler.JobStatus.*;

// A representation of a transfer job submitted to Stork. The entire
// state of the job should be stored in the ad representing this job.
//
// As transfers progress, update ads will be sent by the underlying
// transfer code.
//
// The following fields can come from the transfer module in an update ad:
//   bytes_total - the number of bytes to transfer
//   files_total - the number of files to transfer
//   bytes_done  - indication that some bytes have been transferred
//   files_done  - indication that some files have been transferred
//   complete    - true if success, false if failure

public class Job {
  private int job_id = 0;
  private JobStatus status;
  private Endpoint src, dest;
  //private TransferProgress progress = new TransferProgress();

  private int attempts = 0, max_attempts = 10;
  private String message;

  private Ad options;

  //private Watch queue_timer;
  //private Watch run_timer;

  private transient User user;
  private transient Thread thread;

  // Create and enqueue a new job from a user input ad. Don't give this
  // thing unsanitized user input, because it doesn't filter the user_id.
  // That should be filtered by the caller.
  // TODO: Strict filtering and checking.
  public static Job create(User user, Ad ad) {
    ad.remove("status", "job_id", "attempts");
    ad.rename("src_url",  "src.url");
    ad.rename("dest_url", "dest.url");

    Job j = ad.unmarshalAs(Job.class).status(scheduled);
    j.user = user;

    if (j.src == null || j.dest == null)
      throw new RuntimeException("src or dest was null");
    return j;
  }

  // Gets the job info as an ad, merged with progress ad.
  // TODO: More proper filtering.
  public synchronized Ad getAd() {
    return Ad.marshal(this);
  }

  // Sets the status of the job, updates ad, and adjusts state according to the
  // status. This can also be used to set a message.
  public synchronized JobStatus status() {
    return status;
  } public synchronized Job status(JobStatus s) {
    return status(s, message);
  } public synchronized Job status(String msg) {
    return status(status, msg);
  } public synchronized Job status(JobStatus s, String msg) {
    assert !s.isFilter;

    message = msg;

    // Update state.
    switch (status = s) {
      case scheduled:
        //queue_timer = new Watch(true); break;
      case processing:
        //run_timer = new Watch(true); break;
      case removed:
        if (thread != null)
          thread.interrupt();
      case failed:
      case complete:
        //queue_timer.stop();
        //run_timer.stop();
        //progress.transferEnded(true);
    } return this;
  }

  // Get/set the job id.
  public synchronized int jobId() {
    return job_id;
  } public synchronized void jobId(int id) {
    job_id = id;
  }

  // Called when the job gets removed from the queue.
  public synchronized void remove(String reason) {
    if (isTerminated())
      throw new RuntimeException("The job has already terminated.");
    message = reason;
    status(removed);
  }

  // This will increment the attempts counter, and set the status
  // back to scheduled. It does not actually put the job back into
  // the scheduler queue.
  public synchronized void reschedule() {
    attempts++;
    status(scheduled);
  }

  // Check if the job should be rescheduled.
  public synchronized boolean shouldReschedule() {
    // If we've failed, don't reschedule.
    if (isTerminated())
      return false;

    // Check for custom max attempts.
    if (max_attempts > 0 && attempts >= max_attempts)
      return false;

    // Check for configured max attempts.
    int max = Stork.settings.max_attempts;
    if (max > 0 && attempts >= max)
      return false;

    return true;
  }

  // Run the job and return the status.
  public JobStatus process() {
    run();
    return status;
  }

  // Return whether or not the job has terminated.
  public synchronized boolean isTerminated() {
    switch (status) {
      case failed:
      case pending:
      case complete:
        return true;
      default:
        return false;
    }
  }

  // Run the job and watch it to completion.
  public void run() {
    // Check that the job is scheduled to run.
    synchronized (this) {
      if (status != scheduled) {
        status(failed, "Trying to run unscheduled job.");
        return;
      }
      status(processing);
      thread = Thread.currentThread();
    }

    Resource ss = src.select();
    Resource ds = dest.select();

    // Establish connections to end-points.
    try {
      // If options were given, marshal them into the sessions.
      if (options != null) {
        options.unmarshal(ss);
        options.unmarshal(ds);
      }

      doTransfer(ss, ds);
    } catch (CancellationException e) {
      status(removed);
    } finally {
      thread = null;
    }
  }

  // Do the transfer using the given sessions.
  private void doTransfer(final Resource ss, final Resource ds) {
    URI su = src.uri;
    URI du = dest.uri;

    //ss.select(su).sendTo(ds.select(du));
  }
}
