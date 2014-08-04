package stork.feather.util;

/**
 * A set of functional interfaces for {@code Bell} promises that take Java 8
 * lambdas and method references.
 */
public final class BellHandler {
  private BellHandler() { }

  /** A handler for {@code Bell}s that ring successfully. */
  public static interface Done<T> {
    /** Called when a {@code Bell} rings successfully. */
    public void done(T t) throws Throwable;
  }

  /** A handler for {@code Bell}s that ring fail. */
  public static interface Fail<T> {
    /** Called when a {@code Bell} fails. */
    public void fail(T t) throws Throwable;
  }

  /** A handler for {@code Bell}s that always runs on ring. */
  public static interface Always<T> {
    /** Called when a {@code Bell} rings. */
    public void always() throws Throwable;
  }
}
