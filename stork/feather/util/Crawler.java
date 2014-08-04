package stork.feather.util;

import java.util.*;

import stork.feather.*;

/**
 * A {@code Crawler} crawls and operates on {@code Resource} trees. The
 * crawling proceeds in an order according to some traversal mechanism and
 * optionally produces a result. {@code Crawler} extends {@code Bell}, and will
 * ring with the root {@code Resource} when crawling has finished.
 *
 * @param <R> The {@code Resource} type this {@code Crawler} operates on.
 */
public abstract class Crawler<R extends Resource<?,R>> extends Bell<R> {
  private final R root;
  private final Path pattern;
  private final boolean recursive;
  private int mode = 0;
  private boolean started = false;
  private volatile int count = 0;

  /**
   * Create a {@code Crawler} which will perform operations on the physical
   * resources represented by {@code resource}. {@code resource} may be a
   * non-singleton {@code Resource} in which case all resources it represents
   * will be visited and operated on. Collection resources will not be operated
   * on recursively.
   *
   * @param resource the {@code Resource} to traverse.
   */
  public Crawler(R resource) { this(resource, false); }

  /**
   * Create a {@code Crawler} which will perform operations on the physical
   * resources represented by {@code resource}. {@code resource} may be a
   * non-singleton {@code Resource} in which case all resources it represents
   * will be visited and operated on. Collection resources will be operated on
   * recursively if {@code recursive} is {@code true}.
   *
   * @param resource the {@code Resource} to traverse.
   */
  public Crawler(R resource, boolean recursive) {
    root = resource.trunk();
    pattern = resource.path;
    this.recursive = recursive;
  }

  /** Start the crawling process. */
  public synchronized void start() {
    if (!started)
      crawl(Path.ROOT);
    started = true;
  }

  private synchronized int count(int d) {
    count += d;
    if (count == 0) ring(root);
    return count;
  }

  private void crawl(final Path path) {
    count(1);
    final R resource = root.select(path);
    if (!isDone()) resource.stat().new Promise() {
      public void done(final Stat stat) {
        if (Crawler.this.isDone())
          return;

        // The pattern we'll use for matching below. TODO
        //Path tp = pattern.truncate(resource.path.length()+1);

        // Perform the operation on the resource.
        Bell op = doOperate(path, resource).new Promise() {
          public void done() {
            if (Crawler.this.isDone())
              return;
            // Crawl subresources that match the path.
            if (stat.link == null && stat.files != null) {
              for (Stat s : stat.files) {
                Path sub = resource.path.appendLiteral(s.name);
                crawl(sub);
              }
            }
          } public void always() {
            count(-1);
          }
        };
      } public void fail(Throwable t) {
        if (resource == root)
          Crawler.this.ring(t);
      }
    };
  }

  private Bell doOperate(Path p, R r) {
    Bell b1, b2, b3;
    try { b1 = operate(p, r); } catch (Throwable t) { b1 = null; }
    try { b2 = operate(p);    } catch (Throwable t) { b2 = null; }
    try { b3 = operate(r);    } catch (Throwable t) { b3 = null; }
    return Bell.all(b1, b2, b3);
  }

  /**
   * An operation that will be performed when a {@code Resource} is found.
   * Generally, only one {@code operate(...)} method should be overridden in a
   * subclass.
   *
   * @param path the selection {@code Path} of {@code resource} relative to the
   * root {@code Resource} of the {@code Crawler}.
   * @param resource the {@code Resource} to operate on.
   * @return A {@code Bell} that will ring when the operation is complete, or
   * {@code null} if the operation completed instantly.
   */
  protected Bell operate(Path path, R resource) { return null; }

  /**
   * An operation that will be performed when a {@code Resource} is found.
   * Generally, only one {@code operate(...)} method should be overridden in a
   * subclass.
   *
   * @param path the selection {@code Path} of {@code resource} relative to the
   * root {@code Resource} of the {@code Crawler}.
   * @return A {@code Bell} that will ring when the operation is complete, or
   * {@code null} if the operation completed instantly.
   */
  protected Bell operate(Path path) { return null; }

  /**
   * An operation that will be performed when a {@code Resource} is found.
   * Generally, only one {@code operate(...)} method should be overridden in a
   * subclass.
   *
   * @param resource the {@code Resource} to operate on.
   * @return A {@code Bell} that will ring when the operation is complete, or
   * {@code null} if the operation completed instantly.
   */
  protected Bell operate(R resource) { return null; }

  public static void main(String[] args) {
    String sp = args.length > 0 ? args[0] : "/home/bwross/test";
    final Path path = Path.create(sp);
    final LocalSession s = new LocalSession(path);

    new Crawler<LocalResource>(s.root(), true) {
      long size = 0;

      public Bell operate(final Path path, final LocalResource r) {
        return r.stat().new Promise() {
          public void done(Stat s) { if (s.file) size += s.size; }
          public void fail(Throwable t) { System.out.println(path+" failed..."); }
          public void always() { System.out.println(size); }
        };
      } public void done() {
        System.out.println("Done crawling. Total size: "+size);
      } public void fail(Throwable t) {
        System.out.println("Crawling failed: "+t);
        t.printStackTrace();
      } public void always() {
        s.close();
      }
    }.start();
  }
}
