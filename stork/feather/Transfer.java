package stork.feather;

/**
 * A handle on the state of a data transfer. This class contains methods for
 * controlling data flow and monitoring data throughput. This is the base class
 * for {@code ProxyTransfer} as well as any custom {@code Transfer} controller.
 * <p/>
 * Control operations are performed on a {@code Transfer} through {@code
 * public} {@code Bell} members. This allows implementors to make assumptions
 * about what states certain control methods may be called from.
 *
 * @param <S> the source {@code Resource} type.
 * @param <D> the destination {@code Resource} type.
 */
public abstract class Transfer<S extends Resource, D extends Resource> {
  public final S source;
  public final D destination;

  /**
   * Create a {@code Transfer} from {@code source} to {@code destination}.
   *
   * @param source the source {@code Resource}.
   * @param destination the destination {@code Resource}.
   */
  public Transfer(S source, D destination) {
    this.source = source;
    this.destination = destination;
  }

  /** Ring this {@code Bell} to start the transfer. */
  public final Bell starter = new Bell() {
    public void done() {
      try {
        start();
      } catch (Throwable t) {
        stopper.ring(t);
      }
    } public void fail(Throwable t) {
      stopper.ring(t);
    }
  };

  /** Ring this {@code Bell} to stop the transfer. */
  public final Bell stopper = new Bell() {
    public void always() { stop(); }
  };

  // If we get paused, a bell will be placed here to resume the transfer.
  private Bell<Transfer<S,D>> pauseBell;

  /** Get the source {@code Resource}. */
  public final S source() { return source; }

  /** Get the destination {@code Resource}. */
  public final D destination() { return destination; }

  /**
   * Called when this {@code Transfer} is started. This method may return a
   * {@code Bell} which will ring when the {@code Transfer} is ready, or {@code
   * null} to indicate that it is ready immediately.
   * <p/>
   * Exceptions thrown here or through the returned {@code Bell} will cause the
   * {@code Transfer} to be stopped.
   * <p/>
   * No other transfer methods will be called until this method has completed
   * and the returned {@code Bell}, if non-{@code null}, has rung. This method
   * will be called at most once.
   *
   * @return A {@code Bell} which will ring when the {@code Transfer} has
   * started, or {@code null} if it is ready immediately.
   * @throws Exception if the {@code Transfer} cannot start.
   */
  protected Bell start() throws Exception { return null; }

  /**
   * This is called when the {@code Transfer} has been terminated. Once
   * stopped, the transfer cannot be started again. This may be called before
   * {@link #start()} if the transfer is canceled before it starts.
   */
  protected void stop() { }

  /**
   * Pause the transfer temporarily. {@code resume()} should be called to
   * resume transfer after pausing. Implementors should assume this method will
   * only be called from a running state.
   */
  protected void pause() { }

  /**
   * Resume the transfer after a pause. Implementors should assume this method
   * will only be called from a paused state.
   */
  protected void resume() { }

  /**
   * Check if this {@code Transfer} has not been started nor stopped.
   *
   * @return {@code true} if the transfer is unstarted; {@code false}
   * otherwise.
   */
  public final boolean unstarted() {
    return !starter.isDone();
  }

  /**
   * Check if this {@code Transfer} is in the process of starting.
   *
   * @return {@code true} if the transfer is starting; {@code false} otherwise.
   */
  public final boolean starting() {
    return starter.isDone();
  }

  /** Check if the transfer is complete. */
  public final boolean isDone() {
    return stopper.isDone();
  }

  /**
   * Check if the pipeline is capable of draining {@code Slice}s in arbitrary
   * order. The return value of this method should remain constant across
   * calls.
   *
   * @return {@code true} if transmitting slices in arbitrary order is
   * supported.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public boolean random() { return false; }

  /**
   * Get the number of distinct {@code Resource}s the pipeline may be in the
   * process of transferring simultaneously.
   * <p/>
   * Returning a number less than or equal to zero indicates that an arbitrary
   * number of {@code Resource}s may be transferred concurrently.
   *
   * @return The number of data {@code Resource}s this sink can receive
   * concurrently.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public int concurrency() { return 1; }

  /**
   * Return a {@code Bell} which rings when the {@code Transfer} starts.
   *
   * @return A {@code Bell} which rings when the {@code Transfer} starts.
   */
  public final Bell<Transfer<S,D>> onStart() {
    return starter.as(this);
  }

  /**
   * Return a {@code Bell} which rings when the {@code Transfer} stops.
   *
   * @return A {@code Bell} which rings when the {@code Transfer} stops.
   */
  public final Bell<Transfer<S,D>> onStop() {
    return stopper.as(this);
  }
}
