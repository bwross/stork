package stork;

import stork.ad.*;
import stork.feather.*;
import stork.net.*;
import stork.util.*;
import stork.scheduler.*;

// This is basically a standalone wrapper around Scheduler that lets
// the scheduler run as a standalone process (as opposed to as a client
// of another JVM). This will create server interfaces for the scheduler.

public class StorkServer extends Command {
  public StorkServer() {
    super("server");

    args = new String[] { "[option]..." };
    desc = new String[] {
      "The Stork server is the core of the Stork system, handling "+
        "connections from clients and scheduling transfers. This command "+
        "is used to start a Stork server.",
        "Upon startup, the Stork server loads stork.conf and begins "+
          "listening for clients."
    };
    add('d', "daemonize", "run the server in the background, "+
      "redirecting output to a log file (if specified)");
    add('l', "log", "redirect output to a log file at PATH").new
      SimpleParser("PATH", true);
    add("state", "load/save server state at PATH").new
      SimpleParser("PATH", true);
  }

  public void execute(Ad env) {
    env.unmarshal(Stork.settings);
    env.addAll(Ad.marshal(Stork.settings));

    Scheduler s = Scheduler.start(env);
    URI[] listen = Stork.settings.listen;
    URI web_url = Stork.settings.web_service_url;

    if (listen == null || listen.length < 1)
      listen = new URI[] { Stork.settings.connect };

    // Initialize API endpoints.
    for (URI u : listen) try {
      // Fix shorthand URIs.
      if (u.scheme() == null)
        u = URI.EMPTY.scheme(u.path().name());
      StorkInterface si = StorkInterface.create(s, u);
      Log.info("Listening for ", si.name(), " connections on: "+si.address());
    } catch (Exception e) {
      e.printStackTrace();
      Log.warning("could not create interface: "+e.getMessage());
    }

    // Initialize web server for web documents.
    //if (web_url != null)
      //HTTPInterface.register("web", web_url).start();
  }
}
