package stork.module.ftp;

import io.netty.buffer.*;

import stork.cred.*;
import stork.feather.*;
import stork.module.*;
import stork.scheduler.*;
import stork.util.*;

import static stork.module.ftp.FTPListCommand.*;

public class FTPResource extends Resource<FTPSession, FTPResource> {
  FTPResource(FTPSession session, Path path) {
    super(session, path);
  }

  public synchronized Emitter<String> list() {
    return new Emitter<String>() {{
      final Emitter e = this;
      stat(true).new Promise() {
        public void done(Stat s) {
          String[] names = new String[s.files.length];
          for (int i = 0; i < s.files.length; i++)
            names[i] = s.files[i].name;
          e.emitAll(names);
          e.ring();
        } public void fail(Throwable t) {
          e.ring(t);
        }
      };
    }};
  }

  public synchronized Bell<Stat> stat() {
    return stat(false);
  }

  // Pass true if listing is necessary.
  private synchronized Bell<Stat> stat(final boolean list) {
    return initialize().new AsBell<Stat>() {
      private FTPChannel channel = null;

      // This will be called when initialization is done. It should start the
      // chain reaction that results in listing commands being tried until we
      // find one that works.
      public Bell<Stat> convert(FTPResource me) {
        channel = session.channel;
        tryCommand(FTPListCommand.values()[0]);
        return null;
      }

      // This will call itself until it finds a supported command. If the
      // command is not supported, call itself again with the next available
      // command. If cmd is null, that means we've tried all commands.
      private void tryCommand(final FTPListCommand cmd) {
        if (isDone()) {
          return;
        } if (cmd == null) {
          ring(new Exception("Listing is not supported."));
        } else channel.supports(cmd.toString()).new AsBell<Boolean>() {
          public Bell<Boolean> convert(Boolean supported) {
            if (supported && list)
              return session.cmdCanList(cmd);
            return new Bell<Boolean>(supported);
          } public void done(Boolean supported) {
            if (supported)
              sendCommand(cmd);
            else
              tryCommand(cmd.next());
          } public void fail() {
            tryCommand(cmd.next());
          }
        };
      }

      // A sort of hacky way of dealing with statting directories.
      private Bell<Stat> filterStat(final Stat stat) {
        if (list || stat.dir) {
          return new Bell<Stat>(stat);
        } if (stat.files != null && stat.files.length != 1) {
          stat.dir = true;
          return new Bell<Stat>(stat);
        } if (!path.name().equals(stat.files[0].name)) {
          // List contains different name, assume directory...
          stat.dir = true;
          return new Bell<Stat>(stat);
        } return checkIfDirectory().new As<Stat>() {
          public Stat convert(Boolean dir) {
            return dir ? stat : stat.files[0];
          }
        };
      }

      // Unfortunately FTP has no reliable method for checking if something is
      // a directory without altering the state of the channel...
      private Bell<Boolean> checkIfDirectory() {
        final Bell bell = new Bell();
        channel.new Lock() {{
          new Command("CWD", makePath()).expectComplete().new Promise() {
            public void done() {
              new Command("CWD", path.relativize());
            } public void always() { unlock(); }
          }.promise(bell);
        }};
        return bell.as(true, false);
      }

      // This will get called once we've found a command that is supported.
      // However, if this fails, fall back to the next command.
      private void sendCommand(final FTPListCommand cmd) {
        if (isDone())
          return;

        char hint = cmd.toString().startsWith("M") ? 'M' : 0;
        Stat base = new Stat(name());
        final Bell<Stat> tb = this;
        final FTPListParser parser = new FTPListParser(base, hint) {
          // The parser should ring this bell if it's successful.
          public void done(Stat stat) {
            filterStat(stat).promise(tb);
          } public void fail() {
            // TODO: Check for permanent errors.
            tryCommand(cmd.next());
          }
        };

        parser.name(StorkUtil.basename(path.name()));

        //Log.fine("Trying list command: ", cmd);

        // When doing MLSx listings, we can reduce the response size with this.
        if (hint == 'M' && !session.mlstOptsAreSet) {
          channel.new Command("OPTS MLST Type*;Size*;Modify*;UNIX.mode*");
          session.mlstOptsAreSet = true;
        }

        // Do a control channel listing, if specified.
        if (!cmd.requiresDataChannel())
          channel.new Command(cmd.toString(), makePath()) {
            public void handle(FTPChannel.Reply r) {
              parser.write(r.message().getBytes());
            } public void done(FTPChannel.Reply r) {
              if (r.code/100 > 2) {
                parser.ring(r.asError());
              } else {
                parser.write(r.message().getBytes());
                parser.finish();
              }
            }
          };

        // Otherwise we're doing a data channel listing.
        else channel.new DataChannel() {
          {
            onClose().new Promise() {
              public void always() {
              }
            };
          }
          public Bell init() {
            return channel.new Command(cmd, makePath()).expectComplete();
          } public void receive(Slice slice) {
            parser.write(slice.asBytes());
          }
        };
      }
    };
  }

  // Create a directory at the end-point, as well as any parent directories.
  public Bell<FTPResource> mkdir() {
    if (!isSingleton())
      throw new UnsupportedOperationException();
    return initialize().new AsBell<FTPChannel.Reply>() {
      public Bell<FTPChannel.Reply> convert(FTPResource r) {
        String path = makePath();
        return session.channel.new Command("MKD", path);
      }
    }.new Promise() {
      public void then(FTPChannel.Reply r) {
        // Warning, REALLY unfortunate hack ahead.
        if (r.isComplete() || r.toString().indexOf("exists") >= 0)
          ring();
        else
          ring(new RuntimeException("Could not create directory."));
      }
    }.as(this);
  }

  // Remove a file or directory.
  public Bell<FTPResource> delete() {
    if (!isSingleton())
      throw new UnsupportedOperationException();
    return stat().new AsBell<FTPChannel.Reply>() {
      public Bell<FTPChannel.Reply> convert(Stat stat) {
        if (stat.dir)
          return session.channel.new Command("RMD", makePath());
        else
          return session.channel.new Command("DELE", makePath());
      }
    }.as(this);
  }

  public Sink<FTPResource> sink() {
    return new FTPSink(this);
  }

  public Tap<FTPResource> tap() {
    return new FTPTap(this);
  }

  // Stringify and relativize a path.
  String makePath() {
    String p = path.toString();
    if (p.startsWith("/~"))
      return p.substring(1);
    else if (!p.startsWith("/"))
      return "./"+p;
    return "."+p;
  }
}

/**
 * An FTP {@code Tap} which manages data channels autonomonously.
 */
class FTPTap extends Tap<FTPResource> {
  private FTPChannel.DataChannel dc;

  public FTPTap(FTPResource resource) { super(resource); }

  protected Bell start(final Bell bell) {
    return null;
    /**
    return source().initialize().new AsBell<FTPChannel.DataChannel>() {
      public void then(FTPResource r) {
        dc = source().session.channel.new DataChannel() {
          public Bell init() {
            String path = source().makePath();
            return new Command("RETR", path).expectComplete();
          } public void receive(Slice slice) {  
            pauseUntil(drain(slice));
          }
        }.startWhen(bell);
        dc.onClose().new Promise() {
          public void done()            { finish();  }
          public void fail(Throwable t) { finish(t); }
        };
        return dc.onConnect();
      }
    };
    */
  }
}

/**
 * An FTP {@code Sink} which manages data channels autonomonously.
 */
class FTPSink extends Sink<FTPResource> {
  private FTPChannel.DataChannel dc;
  public FTPSink(FTPResource resource) { super(resource); }

  protected Bell start() {
    return destination().initialize().new AsBell<FTPChannel.DataChannel>() {
      public Bell<FTPChannel.DataChannel> convert(FTPResource r) {
        dc = destination().session.channel.new DataChannel() {
          public Bell init() {
            String path = destination().makePath();
            return new Command("STOR", path).expectComplete();
          }
        };
        return dc.onConnect();
      }
    };
  }

  public Bell drain(final Slice slice) {
    return dc.send(slice);
  }

  public void finish()            { dc.close(); }
  public void finish(Throwable t) { dc.close(); }
}
