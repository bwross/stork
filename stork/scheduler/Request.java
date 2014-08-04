package stork.scheduler;

import java.util.*;

import stork.ad.*;
import stork.feather.*;

/**
 * A request made to the scheduler. It will be rung with the response when the
 * request has been fulfilled. The response object used to ring this 
 */
public class Request extends Bell<Object> {
  public Ad ad;
  public String command;
  public User user;
  public Scheduler.Handler handler;

  /**
   * Create a new request based on {@code ad}.
   *
   * @param ad the {@code Ad} to create a request from.
   */
  public Request(Ad ad) {
    command = ad.get("command");
    this.ad = ad.remove("command");
  }
}
