package stork.scheduler;

import stork.*;
import stork.ad.*;
import stork.cred.*;
import stork.util.*;
import stork.feather.*;
import stork.module.ftp.*;
import stork.util.*;

import java.io.*;
//import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * The Stork transfer job scheduler. Maintains its own internal configuration
 * and state as an ad. Operates based on commands given to it in the form of
 * ads. Can be run standalone and receive commands from a number of interfaces,
 * or can be run as part of a larger program and be given commands directly (or
 * both).
 *
 * The entire state of the scheduler can be serialized and saved to disk, and
 * subsequently recovered if so desired.
 *
 * TODO: This thing could use some refactoring.
 */
public class Scheduler {
  public static volatile Scheduler instance;

  public Ad env = new Ad();
  public User.Map users = new User.Map();
  public CredManager creds = new CredManager();

  public transient LinkedBlockingQueue<Job> jobs =
    new LinkedBlockingQueue<Job>();

  private transient StorkQueueThread[]  thread_pool;
  private transient StorkWorkerThread[] worker_pool;
  private transient DumpStateThread dump_state_thread;

  private transient Map<String, Handler> cmd_handlers;
  public transient ModuleTable modules;

  private transient LinkedBlockingQueue<Request> req_queue =
    new LinkedBlockingQueue<Request>();

  private transient User anonymous = User.anonymous();

  // Map of idle sessions, for session reuse.
  private transient Map<Session, Session> session_pool =
    Collections.synchronizedMap(new HashMap<Session, Session>());

  // Map of ongoing listings, for request aggregation.
  private transient Map<Resource, Bell<Stat>> ls_aggregator =
    new ConcurrentHashMap<Resource, Bell<Stat>>();

  static {
    // Register a handler to marshal endpoints.
    new Ad.Marshaller<Endpoint>(Endpoint.class) {
      public Endpoint unmarshal(String uri) {
        return new Endpoint(uri);
      }
    };
  }

  // Put a job into the scheduling queue.
  public void schedule(Job job) {
    jobs.add(job);
  }

  // It's like a thread, but storkier.
  private abstract class StorkThread<O> extends Thread {
    // Set these at any time to control the thread.
    public volatile boolean dead = false;
    public volatile boolean idle = true;

    public StorkThread(String name) {
      super(name);
      setDaemon(true);
      start();
    }

    public final void run() {
      while (!dead) try {
        O j = getAJob();
        idle = false;
        execute(j);
      } catch (Exception e) {
        continue;
      } finally {
        idle = true;
      }
    }

    public abstract O getAJob() throws Exception;
    public abstract void execute(O work);
  }

  // A thread which runs continuously and starts jobs as they're found.
  private class StorkQueueThread extends StorkThread<Job> {
    StorkQueueThread() {
      super("stork queue thread");
    }

    public Job getAJob() throws Exception {
      return jobs.take();
    }

    // Continually remove jobs from the queue and start them.
    public void execute(Job job) {
      Log.info("Pulled job from queue: "+job);

      // Run the job then check the return status.
      switch (job.process()) {
        // If a job is still processing, something weird happened.
        case processing:
          throw new RuntimeException("job still processing after completion");
          // If job is scheduled, put it back in the schedule queue.
        case scheduled:
          Log.info("Job "+job.jobId()+" rescheduling...");
          schedule(job); break;
          // If the job was paused, put it in limbo until it's resumed.
        case paused:  // This can't happen yet!
          break;
          // Alert the user if it failed.
        case failed:
          Log.info("Job "+job.jobId()+" failed!");
      } dumpState();
    }
  }

  // A thread which handles client requests.
  private class StorkWorkerThread extends StorkThread<Request> {
    StorkWorkerThread() {
      super("stork worker thread");
    }

    public Request getAJob() throws Exception {
      return req_queue.take();
    }

    // Continually remove jobs from the queue and start them.
    public void execute(final Request req) {
      Log.fine("Worker pulled request from queue: ", req.command);

      // Try handling the command if it's not done already.
      if (!req.isDone()) try {
        if (req.command == null)
          throw new RuntimeException("No command specified.");

        // Check if user information was provided.
        if (req.ad.has("user")) try {
          req.user = users.login(req.ad.getAd("user"));
        } catch (Exception e) {
          if (req.handler.requiresLogin())
            throw new RuntimeException("Action requires login.");
          req.user = anonymous;
        } else {
          req.user = anonymous;
        }

        // Let the magic happen.
        Bell bell = req.handler.handle(req);

        // Limit request time.
        double deadline = env.getDouble("request_timeout", 0);
        if (deadline > 0)
          req.deadline(deadline);

        bell.promise(req).promise(new Bell() {
          public void always() {
            // Save state if we affected it.
            if (req.handler.affectsState(req))
              dumpState();
            Log.fine("Done with request: ", req.command);
          }
        });
      } catch (Exception e) {
        e.printStackTrace();
        req.ring(e);
      } else {
        Log.fine("Request pulled from queue was cancelled.");
      }
    }
  }

  // Stork command handlers should implement this interface.
  public abstract class Handler {
    public abstract Bell handle(Request req);

    // Override this for commands that either don't require logging in or
    // always require logging in.
    public boolean requiresLogin() {
      return env.getBoolean("registration");
    }

    // Override this is the action affects server state.
    public boolean affectsState(Request req) {
      return affectsState();
    } public boolean affectsState() {
      return false;
    }
  }

  class StorkQHandler extends Handler {
    public Bell handle(Request req) {
      Bell bell = new Bell();
      List l = new JobSearcher(req.user.jobs).query(req.ad);

      return bell.ring(req.ad.getBoolean("count") ?
        new Ad("count", l.size()) : Ad.marshal(req.user.jobs));
    }
  }

  class StorkMkdirHandler extends Handler {
    public Bell handle(Request req) {
      Endpoint ep = req.ad.unmarshalAs(Endpoint.class);
      return ep.select().mkdir();
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  class StorkRmfHandler extends Handler {
    public Bell handle(Request req) {
      Endpoint ep = req.ad.unmarshalAs(Endpoint.class);
      return ep.select().delete();
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  class StorkLsHandler extends Handler {
    public synchronized Bell handle(Request req) {
      final Endpoint ep = req.ad.unmarshalAs(Endpoint.class);
      Resource nr = ep.select();

      // See if there's an existing session we can reuse.
      Session session = session_pool.remove(nr.session);
      if (session != null && !session.isClosed()) {
        Log.fine("Reusing existing session: ", session);
        nr = nr.reselectOn(session);
      } else {
        final Session s = nr.session;
        Log.fine("Using new session: ", s);
        s.onClose(new Bell() {
          public void always() {
            Log.fine("Removing from session pool: ", s);
            synchronized (session_pool) {
              session_pool.remove(s);
            }
          }
        });
      }

      final Resource res = nr;

      // See if there is an on-going listing request.
      Bell<Stat> listing = ls_aggregator.get(res);
      if (listing != null) {
        Log.fine("Waiting on existing list request...");
        return listing;
      }

      listing = res.stat();

      // Register the ongoing listing.
      ls_aggregator.put(res, listing);
      listing.new Promise() {
        public void always() {
          ls_aggregator.remove(res);
        }
      };

      // Put the session back when we're done.
      listing.new Promise() {
        public void always() {
          Session s = res.session;
          synchronized (session_pool) {
            if (!s.isClosed() && !session_pool.containsKey(s))
              session_pool.put(s, s);
          }
          ls_aggregator.remove(res);
        }
      };

      return listing;
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  // Handle user registration.
  class StorkUserHandler extends Handler {
    public Bell handle(Request req) {
      Bell bell = new Bell();
      if ("register".equals(req.ad.get("action"))) {
        User su = users.register(req.ad);
        Log.info("Registering user: ", su.email);
        return bell.ring(su.toAd());
      } if ("login".equals(req.ad.get("action"))) {
        return bell.ring(users.login(req.ad).toAd());
      } if ("history".equals(req.ad.get("action"))) {
        if (req.ad.has("uri")) try {
          req.user.addHistory(URI.create(req.ad.get("uri")));
        } catch (Exception e) {
          throw new RuntimeException("Could not parse URI...");
        } return bell.ring(req.user.history);
      } return bell.ring(req.user.toAd());
    }

    public boolean requiresLogin() {
      return false;
    }

    public boolean affectsState(Request req) {
      return "register".equals(req.ad.get("action"));
    } public boolean affectsState() {
      return true;
    }
  }

  class StorkSubmitHandler extends Handler {
    public Bell handle(Request req) {
      Job job = Job.create(req.user, req.ad);

      // Schedule the job to execute and add the job to the user context.
      schedule(job);

      synchronized (req.user) {
        req.user.jobs.add(job);
        job.jobId(req.user.jobs.size());
      }

      return new Bell().ring(job.getAd());
    }

    public boolean affectsState() {
      return true;
    }
  }

  class StorkRmHandler extends Handler {
    public Bell handle(Request req) {
      Range r = new Range(req.ad.get("range"));
      Range sdr = new Range(), cdr = new Range();

      if (r.isEmpty())
        throw new RuntimeException("No jobs specified.");

      // Find ad in job list, set it as removed.
      List<Job> list = new JobSearcher(req.user.jobs).query(req.ad);
      for (Job j : list) try {
        j.remove("removed by user");
        sdr.swallow(j.jobId());
      } catch (Exception e) {
        Log.info("Couldn't remove job ", j.jobId(), ": ", e.getMessage());
        cdr.swallow(j.jobId());
      }

      // See if there's anything in our "couldn't delete" range.
      if (sdr.isEmpty())
        throw new RuntimeException("No jobs were removed.");
      Ad ad = new Ad("removed", sdr.toString());
      if (!cdr.isEmpty())
        ad.put("not_removed", cdr.toString());
      return new Bell().ring(ad);
    }

    public boolean affectsState() {
      return true;
    }
  }

  class StorkInfoHandler extends Handler {
    // Send transfer module information.
    Ad sendModuleInfo(Request req) {
      return Ad.marshal(modules.infoAds());
    }

    // Send server information. But for now, don't send anything until we
    // know what sort of information is good to send.
    Ad sendServerInfo(Request req) {
      Ad ad = new Ad();
      ad.put("version", Stork.version());
      ad.put("commands", new Ad(cmd_handlers.keySet()));
      return ad;
    }

    // Send information about a credential or about all credentials.
    Ad sendCredInfo(Request req) {
      String uuid = req.ad.get("cred");
      if (uuid != null) try {
        return creds.getCred(uuid).getAd();
      } catch (Exception e) {
        throw new RuntimeException("no credential could be found");
      } else {
        return Ad.marshal(creds.getCredInfo(req.user.creds));
      }
    }

    public Bell handle(Request req) {
      String type = req.ad.get("type", "module");

      if (type.equals("module"))
        return new Bell().ring(sendModuleInfo(req));
      if (type.equals("server"))
        return new Bell().ring(sendServerInfo(req));
      if (type.equals("cred"))
        return new Bell().ring(sendCredInfo(req));
      throw new RuntimeException("Invalid type: "+type);
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  // Handles creating credentials.
  class StorkCredHandler extends Handler {
    public Bell handle(Request req) {
      String action = req.ad.get("action");

      if (action == null) {
        throw new RuntimeException("no action specified");
      } if (action.equals("create")) {
        StorkCred<?> cred = req.ad.unmarshalAs(StorkCred.class);
        String uuid = creds.add(cred);
        req.user.creds.add(uuid);
        return new Bell().ring(cred.getAd().put("uuid", uuid));
      } throw new RuntimeException("invalid action");
    }

    public boolean affectsState() {
      return true;
    }
  }

  // Iterate over libexec directory and add transfer modules to list.
  public void populateModules() {
    // Load built-in modules.
    // TODO: Automatic discovery for built-in modules.
    //modules.register(new GridFTPModule());
    //modules.register(new SFTPModule());
    modules.register(new FTPModule());
    if (env.has("libexec"))
      modules.registerDirectory(new File(env.get("libexec")));

    // Check if anything got added.
    if (modules.modules().isEmpty())
      Log.warning("no transfer modules registered");
  }

  // Initialize the thread pool according to config.
  // TODO: Replace worker threads with asynchronous I/O.
  public void initThreadPool() {
    int jn = env.getInt("max_jobs", 10);
    int wn = env.getInt("workers", 4);

    if (jn < 1) {
      jn = 10;
      Log.warning("invalid value for max_jobs, "+
                  "defaulting to "+jn);
    } if (wn < 1) {
      wn = 4;
      Log.warning("invalid value for workers, "+
                  "defaulting to "+wn);
    }

    thread_pool = new StorkQueueThread[jn];
    worker_pool = new StorkWorkerThread[wn];
    
    Log.info("Starting "+jn+" job threads, and "+wn+" worker threads...");

    for (int i = 0; i < thread_pool.length; i++) {
      thread_pool[i] = new StorkQueueThread();
    } for (int i = 0; i < worker_pool.length; i++) {
      worker_pool[i] = new StorkWorkerThread();
    }

    // Let's go ahead and start one of these things. This is a quick hack
    // to make sure worker threads don't get stuck on long calls.
    new Thread("stork sentinel") {
      public void run() {
        while (true) try {
          // Sleep for a bit and make sure there's a free thread.
          sleep(100);
          check(worker_pool, StorkWorkerThread.class);
          //check(thread_pool, StorkQueueThread.class);
        } catch (Exception e) {
          // I doubt this will happen.
        }
      } public <C extends StorkThread<?>> void check(C[] pool, Class<C> c) {
        for (C t : pool) if (t.idle) return;
        int i = (int) (Math.random() * pool.length);
        Log.info("Retiring ", c, " #", i);
        C z;
        if (c == StorkWorkerThread.class)
          z = c.cast(new StorkWorkerThread());
        else if (c == StorkQueueThread.class)
          z = c.cast(new StorkQueueThread());
        else
          return;
        pool[i].dead = true;
        pool[i] = z;
        Log.info("Created new ", c);
      }
    }.start();
  }

  // Put a command in the server's request queue.
  public Request putRequest(Ad ad) {
    return putRequest(new Request(ad));
  } public Request putRequest(Request rb) {
    assert rb.ad != null;
    rb.handler = handler(rb.command);
    if (rb.handler == null) {
      rb.ring(new Exception("Invalid command: "+rb.command));
    } else try {
      Log.fine("Enqueuing request: "+rb.ad);
      req_queue.add(rb);
    } catch (Exception e) {
      // This can happen if the queue is full. Which right now it never
      // should be, but who knows.
      Log.warning("Rejecting request: ", rb.ad);
      rb.ring(new Exception("Rejected request"));
    } return rb;
  }

  // Get the handler for a command.
  public Handler handler(String cmd) {
    return cmd_handlers.get(cmd);
  }

  // Force the state dumping thread to dump the state.
  private synchronized Scheduler dumpState() {
    dump_state_thread.interrupt();
    return this;
  }

  // Thread which dumps server state periodically, or can be forced
  // to dump the server state.
  private class DumpStateThread extends Thread {
    private boolean dead = false;

    public DumpStateThread() {
      super("server dump thread");
      setDaemon(true);
    }

    public void kill() {
      dead = true;
      interrupt();
    }

    public void run() {
      while (!dead) {
        int delay = env.getInt("state_save_interval", 120);
        if (delay < 1) delay = 1;

        // Wait for the delay, then dump the state. Can be interrupted to dump
        // the state early.
        try {
          sleep(delay*1000);
        } catch (Exception e) {
          // Ignore.
        } if (!dead) {
          dumpState();
        }
      }
    }

    // Dump the state to the state file.
    private synchronized void dumpState() {
      String state_path = env.get("state_file");
      File state_file = null, temp_file = null;
      PrintWriter pw = null;

      if (state_path != null) try {
        state_file = new File(state_path).getAbsoluteFile();

        // Some initial sanity checks.
        if (state_file.exists()) {
          if (state_file.exists() && !state_file.isFile())
            throw new RuntimeException("state file is a directory");
          if (!state_file.canWrite())
            throw new RuntimeException("cannot write to state file");
        }

        temp_file = File.createTempFile(
          ".stork_state", "tmp", state_file.getParentFile());
        pw = new PrintWriter(temp_file, "UTF-8");

        pw.print(Ad.marshal(Scheduler.this).toJSON());
        pw.close();
        pw = null;

        if (!temp_file.renameTo(state_file))
          throw new RuntimeException("could not rename temp file");
      } catch (Exception e) {
        Log.warning("couldn't save state: "+
                           state_file+": "+e.getMessage());
      } finally {
        if (temp_file != null && temp_file.exists()) {
          temp_file.deleteOnExit();
          temp_file.delete();
        } if (pw != null) try {
          pw.close();
        } catch (Exception e) {
          // Ignore.
        }
      }
    }
  }

  // Set the path for the state file.
  public synchronized Scheduler setStateFile(File f) {
    if (f != null)
      env.put("state_file", f.getAbsolutePath());
    else
      env.remove("state_file");
    return this;
  }

  // Unmarshal data from an ad.
  private void unmarshalFrom(Ad ad) {
    // Add all of the users.
    Ad ua = ad.getAd("users");
    if (ua != null) for (String s : ua.keySet()) {
      System.out.println(ua.getAd(s));
      User u = new User(ua.getAd(s));
      users.insert(u);

      // Add their unfinished jobs.
      if (u.jobs != null) for (Job j : u.jobs)
        if (!j.isTerminated()) schedule(j.status(JobStatus.scheduled));
    }
  }

  // Load server state from a file.
  public Scheduler loadServerState(String f) {
    return loadServerState(f != null ? new File(f) : null);
  } public Scheduler loadServerState(File f) {
    if (f != null && f.exists()) {
      Log.info("Loading server state file: "+f);
      return loadServerState(Ad.parse(f));
    } return this;
  } public Scheduler loadServerState(Ad state) {
    try {
      unmarshalFrom(state);
    } catch (Exception e) {
      Log.warning("Couldn't load server state: "+e.getMessage());
      e.printStackTrace();
    } return this;
  }

  // start() will return the singleton instance.
  public static Scheduler instance() {
    return start();
  }

  // Restart a scheduler from saved state.
  public static Scheduler restart(String s) {
    return restart(new File(s));
  } public static synchronized Scheduler restart(File f) {
    if (instance != null)
      instance.kill();
    instance = null;
    return start(f).init();
  }

  // Start a new scheduler with an optional config environment.
  public static Scheduler start() {
    return start((Ad)null);
  } public static Scheduler start(File f) {
    return start(Ad.parse(f));
  } public static synchronized Scheduler start(Ad env) {
    if (instance != null)
      return instance;

    Scheduler s = new Scheduler();

    if (env == null)
      env = new Ad();
    else if (env.has("state_file"))
      s.loadServerState(env.get("state_file"));

    s.env = env;

    return instance = s.init();
  }

  private Scheduler init() {
    // Initialize command handlers
    cmd_handlers = new HashMap<String, Handler>();
    cmd_handlers.put("q", new StorkQHandler());
    cmd_handlers.put("ls", new StorkLsHandler());
    cmd_handlers.put("mkdir", new StorkMkdirHandler());
    cmd_handlers.put("rmf", new StorkRmfHandler());
    cmd_handlers.put("status", new StorkQHandler());
    cmd_handlers.put("submit", new StorkSubmitHandler());
    cmd_handlers.put("rm", new StorkRmHandler());
    cmd_handlers.put("info", new StorkInfoHandler());
    cmd_handlers.put("user", new StorkUserHandler());
    cmd_handlers.put("cred", new StorkCredHandler());

    // Initialize transfer module set
    modules = ModuleTable.instance();

    // Initialize workers
    populateModules();
    initThreadPool();

    dump_state_thread = new DumpStateThread();
    dump_state_thread.start();
    dumpState();

    Log.info("Server state: "+Ad.marshal(this));

    return this;
  }

  // TODO: Kill the scheduler and free any resources.
  private void kill() {
    for (StorkThread t : thread_pool)
      t.dead = true;
    for (StorkThread t : worker_pool)
      t.dead = true;
    dump_state_thread.kill();
  }

  // Don't allow these to be created willy-nilly.
  private Scheduler() {
    if (instance != null)
      throw new Error("A Stork scheduler has already been instantiated");
  }
}
