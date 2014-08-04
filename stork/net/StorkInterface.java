package stork.net;

import java.net.*;

import stork.ad.*;
import stork.scheduler.*;
import stork.feather.*;
import stork.feather.URI;

/**
 * An interface which awaits incoming client requests and passes them on to the
 * scheduler.
 */
public abstract class StorkInterface {
  private final Scheduler scheduler;

  /**
   * Create a {@code StorkInterface} for the given scheduler.
   *
   * @param scheduler the scheduler to provide an interface for.
   */
  public StorkInterface(Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  /**
   * Automatically determine and create an interface from a URI.
   *
   * @return A {@code StorkInterface} listening for connection at endpoint
   * specified by {@code uri}.
   * @param scheduler the scheduler to provide an interface for.
   * @param uri the URI specifying the interface to listen on.
   * @throws RuntimeException if an interface could not be created based on the
   * URI.
   */
  public static StorkInterface create(Scheduler scheduler, URI uri) {
    String proto = uri.scheme();

    if (proto == null)  // Hack for standalone scheme names.
      proto = (uri = URI.create(uri+"://")).scheme();
    if (proto == null)
      throw new RuntimeException("Invalid interface descriptor: "+uri);
    if (proto.equals("tcp"))
      return new TCPInterface(scheduler, uri);
    if (proto.equals("http") || proto.equals("https"))
      return new HTTPInterface(scheduler, uri);
    throw new RuntimeException(
      "Unsupported interface scheme ("+proto+") in "+uri);
  }

  /**
   * Get the name of this interface. This is used for logging purposes.
   *
   * @return the name of this interface.
   */
  public abstract String name();

  /**
   * Get a description of the address this interface is listening on. This is
   * used for logging purposes.
   *
   * @return the description of this interface's address.
   */
  public abstract String address();

  /**
   * Used by subclasses to issue a request ad to the scheduler asynchronously.
   * This delegates to {@code issueRequest(Ad)} asynchronously, and returns the
   * associated {@code Request} object asynchronously through a bell.
   *
   * @param request (via bell) the request in the form of an ad.
   * @return (via bell) The enqueued {@link Request} object returned by the
   * scheduler.
   */
  protected Bell<Request> issueRequest(Bell<Ad> request) {
    return request.new As<Request>() {
      public Request convert(Ad request) {
        return issueRequest(request);
      }
    };
  }

  /**
   * Used by subclasses to issue a request ad to the scheduler. The {@code
   * Request} object returned by this method is a bell which will be rung with
   * the response object. The subclass should promise the request to a handler
   * which will appropriately marshal the response object.
   *
   * @param request the request in the form of an ad.
   * @return The enqueued {@link Request} object returned by the scheduler.
   */
  protected Request issueRequest(Ad request) {
    return scheduler.putRequest(request);
  }

  /**
   * Get the scheduler handler for a command.
   */
  protected Scheduler.Handler handler(String command) {
    return scheduler.handler(command);
  }

  /**
   * Create an ad representing a {@code Throwable}.
   *
   * @param throwable a {@code Throwable} to return an ad representing.
   * @return An ad representing {@code throwable}.
   */
  public static Ad errorToAd(final Throwable throwable) {
    if (throwable == null) {
      return errorToAd(new NullPointerException());
    } return Ad.marshal(new Object() {
      String type = throwable.getClass().getSimpleName();
      String message = message(throwable);

      String message(Throwable t) {
        if (t == null) return null;
        String m = t.getLocalizedMessage();
        return m != null ? m : message(t.getCause());
      }
    });
  }
}
