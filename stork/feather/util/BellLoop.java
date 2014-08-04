package stork.feather.util;

import stork.feather.*;

/**
 * An experimental {@code Bell}-driven loop.
 */
public abstract class BellLoop extends Bell {
  private final Object lock;

  /** Create a {@code BellLoop} synchronized on itself. */
  public BellLoop() { lock = this; }

  /**
   * Create a {@code BellLoop} synchronized on {@code lock}. Everytime looping
   * is done, the {@code lock} will be synchronized on.
   */
  public BellLoop(Object lock) { this.lock = lock; }

  /**
   * Start looping immediately.
   */
  public void start() { start(null); }

  /**
   * Start looping when {@code bell} rings. If {@code bell} is {@code null},
   * start immediately.
   */
  public void start(Bell bell) {
    if (bell == null)
      bell = Bell.rungBell();
    bell.promise(this);
  }

  /**
   * When a {@code Bell} this {@code BellLoop} is promised to rings, start the
   * loop.
   */
  protected final void then(Object o) {
    doIterate();
  }

  private void doIterate() {
    Bell bell = null;
    synchronized (lock) {
      bell = lock();
    } if (bell == null) {
      bell = Bell.rungBell();
    } if (!isDone()) bell.new Promise() {
      public void then(Object o) throws Exception {
        if (isDone()) {
          return;
        } if (condition()) {
          body();
          doIterate();
        } else {
          BellLoop.this.ring();
        }
      } public void fail(Throwable t) {
        BellLoop.this.ring(t);
      }
    };
  }

  /**
   * Return a {@code Bell} that will ring when the next iteration of the loop
   * should run. If {@code null} is returned, the body executes immediately.
   */
  public abstract Bell lock();

  /** Return {@code true} as long as the loop should run. */
  public boolean condition() { return true; }

  /**
   * The iteration operation performed by the loop. This method should return a
   * {@code Bell} that will ring when the next iteration should be performed.
   * This method may stop the loop by ringing the {@code BellLoop}. Otherwise,
   * the loop will be stopped when {@code condition()} is {@code false}.
   */
  public abstract void body() throws Exception;
}
