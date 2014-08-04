package stork;

import java.util.*;
import java.io.*;

import stork.ad.*;
import stork.client.*;
import stork.feather.*;
import stork.scheduler.*;
import stork.util.*;

// The stork command without any arguments.
//
// Handles parsing config files and command line arguments (or rather
// handles delegating command line parsing, to be precise) and initing
// and running Stork commands according to passed arguments.

public class Stork extends Command {
  public static final Settings settings = new Settings();
  private static String version = null;

  static {
    // Register a handler to marshal strings in ads into Feather URIs.
    new Ad.Marshaller<URI>(URI.class) {
      public String marshal(URI uri) {
        return uri.toString();
      } public URI unmarshal(String uri) {
        return URI.create(uri);
      }
    };
  }

  // A class for storing Stork configuration settings.
  public static class Settings {
    public int max_jobs = 10;
    public int max_attempts = 10;
    public int max_history = 10;

    public String libexec = "libexec";

    public String state_file = null;
    public int state_save_interval = 120;

    public URI connect = URI.create("tcp://localhost:57024");
    public URI[] listen;
    public URI web_service_url;

    public boolean registration = false;

    public double request_timeout = 5.0;
  }

  // Try to get the version and build time from the build tag.
  public static String version() {
    if (version != null)
      return version;
    String app = "Stork", ver = "", bts = "unknown";
    try {
      Properties props = new Properties();
      props.load(Stork.class.getResourceAsStream("/build_tag"));
      app = props.getProperty("appname", app);
      ver = props.getProperty("version", ver);
      bts = props.getProperty("buildtime", bts);
    } catch (Exception e) {
      // Who cares...
    } return version = StorkUtil.join(app, ver, '('+bts+')');
  }

  public Stork() {
    super("stork");

    // Construct the bare command line parser.
    args = new String[] { "<command> [args]", "[option...]" };

    // Add command options.
    add('V', "version", "display the version number and exit").new
      Parser<Void>() {
        public Void handle() {
          System.out.println(version());
          System.exit(0);
          return null;
        }
      };
    // FIXME: This is a little broken because it reads the default
    // config file first.
    add('C', "conf", "specify custom path to stork.conf").new
      Parser<Void>("PATH", true) {
        public Void handle(String arg) {
          loadConfig(arg);
          return null;
        }
      };
    add('q', "quiet", "don't print anything to standard output");

    // Register subcommands.
    add("server", "start the Stork server", StorkServer.class);
    add("q", "query the Stork queue", StorkQ.class);
    add("status", "an alias for the q command", StorkQ.class);
    add("rm", "cancel or unschedule a job", StorkRm.class);
    add("ls", "retrieve a directory listing", StorkLs.class);
    add("info", "view Stork server settings", StorkInfo.class);
    add("raw", "send commands for debugging", StorkRaw.class);
    add("submit", "submit a job to the server", StorkSubmit.class);
    add("user", "log in or register", stork.client.StorkUser.class);
    add("cred", "register a credential", stork.client.StorkCred.class);

    foot = new String[] {
      "Stork is still undergoing testing and development. "+
      "If you encounter any bugs, please file an issue report at "+
      "<https://github.com/didclab/stork/issues>.", version()
    };
  }

  // Default locations for stork.conf.
  private static String[] default_paths = {
    System.getenv("STORK_CONFIG"),
    System.getProperty("stork.config"),
    System.getProperty("stork.exedir", ".")+"/stork.conf",
    "./stork.conf",
    "../stork.conf",
    "/usr/local/stork/stork.conf"
  };

  // Check default paths until we find a readable file. Null if none found.
  private static File defaultConfig() {
    File f;
    for (String s : default_paths) if (s != null) {
      f = checkPath(s);
      if (f != null) return f;
    } return null;
  }

  // Check that path is readable and return absolutized file, else null.
  private static File checkPath(String path) {
    File file = new File(path).getAbsoluteFile();
    return file.canRead() ? file : null;
  }

  // Parse config file, where each line is either a comment or an
  // Ad expression which gets merged into the returned ad.
  private static Ad parseConfig(String path) {
    File file = (path != null) ? checkPath(path) : defaultConfig();

    // Error checking
    if (file == null) {
      if (path != null)
        throw new RuntimeException("couldn't open '"+path+"'");
      throw new RuntimeException("STORK_CONFIG not set and "+
              "couldn't find stork.conf in default locations");
    } else if (!file.canRead()) {
      Log.warning("couldn't open config file '"+file+"'");
      return new Ad();
    }

    return Ad.parse(file, true);
  }

  // Find the config file, open and parse it, and unmarshal settings.
  private static void loadConfig(String path) {
    parseConfig(path).unmarshal(settings);
  }

  public static void main(String[] args) {
    loadConfig(null);
    new Stork().parseAndExecute(args);
  }
}
