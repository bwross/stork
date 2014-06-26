package stork.feather.util;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

import stork.feather.*;

/** A {@code Resource} produced by a {@code LocalSession}. */
public class LocalResource extends Resource<LocalSession,LocalResource> {
  // Separate reference to work around javac bug.
  LocalSession session;

  public LocalResource(LocalSession session, Path path) {
    super(session, path);
    this.session = session;
  }

  // Get the absolute path to the file.
  private Path path() {
    return path.absolutize(session.path);
  }

  // Get the File based on the path.
  public File file() {
    Path p = session.path.append(path);
    return new File(p.toString());
  }

  public Bell<LocalResource> mkdir() {
    return new ThreadBell(session.executor) {
      public Object run() {
        File file = file();
        if (file.exists() && !file.isDirectory())
          throw new RuntimeException("Resource is a file.");
        else if (!file.exists() && !file.mkdirs())
          throw new RuntimeException("Could not create directory.");
        return null;
      }
    }.start().as(this);
  }

  public Bell<LocalResource> delete() {
    return new ThreadBell(session.executor) {
      private File root = file();

      public Object run() {
        remove(root);
        return null;
      }

      // Recursively remove files.
      private void remove(File file) {
        if (isCancelled()) {
          throw new java.util.concurrent.CancellationException();
        } if (!file.exists()) {
          if (file == root)
            throw new RuntimeException("Resource does not exist: "+file);
        } else try {
          // If not a symlink and is a directory, delete contents.
          if (file.getCanonicalFile().equals(file) && file.isDirectory())
            for (File f : file.listFiles()) remove(f);
          if (!file.delete() && file == root)
            throw new RuntimeException("Resource could not be deleted: "+file);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }.start().as(this).detach();
  }

  // Returns the target File if this is a symlink, null otherwise.
  private File resolveLink(File file) {
    try {
      File cf = file.getCanonicalFile();
      if (!file.equals(cf))
        return cf;
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  public Bell<Stat> stat() {
    return new ThreadBell<Stat>(session.executor) {
      { string = path().toString(); }
      public Stat run() {
        File file = file();

        if (!file.exists())
          throw new RuntimeException("Resource does not exist: "+file);

        Stat stat = new Stat(file.getName());
        stat.size = file.length();
        stat.file = file.isFile();
        stat.dir = file.isDirectory();
        
        File sym = resolveLink(file);
        if (sym != null)
          stat.link = Path.create(file.toString());
        stat.time = file.lastModified();
        return stat;
      }
    }.start().detach();
  }

  public Emitter<String> list() {
    final Emitter<String> emitter = new Emitter<String>();
    new ThreadBell<String>(session.executor) {
      { string = path().toString(); }
      public String run() {
        File file = file();

        if (!file.exists())
          throw new RuntimeException("Resource does not exist: "+file);
        if (!file.isDirectory())
          throw new RuntimeException("Resource is not a directory: "+file);
        File sym = resolveLink(file);
        if (sym != null)
          throw new RuntimeException("Resource is not a directory: "+file);

        String[] files = file.list();
        if (files == null)
          throw new RuntimeException("Resource is not a directory: "+file);

        emitter.emitAll(files);
        return null;
      }
    }.start().promise(emitter);
    return emitter;
  }

  public Tap<LocalResource> tap() {
    return new LocalTap(this);
  }

  public Sink<LocalResource> sink() {
    return new LocalSink(this);
  }
}

class LocalTap extends Tap<LocalResource> {
  final File file = source().file();
  private volatile Bell pause;
  private RandomAccessFile raf;
  private FileChannel channel;
  private long offset = 0, remaining = 0;
  private long chunkSize = 4096;

  // Small hack to take advantage of NIO features.
  private WritableByteChannel nioToFeather = new WritableByteChannel() {
    public int write(ByteBuffer buffer) {
      Slice slice = new Slice(buffer);
      pause = drain(slice);
      return slice.length();
    }

    public void close() { }
    public boolean isOpen() { return true; }
  };

  // State of the current transfer.
  public LocalTap(LocalResource root) { super(root); }

  public Bell start(Bell bell) throws Exception {
    if (!file.exists())
      throw new RuntimeException("File not found");
    if (!file.canRead())
      throw new RuntimeException("Permission denied");
    if (!file.isFile())
      throw new RuntimeException("Resource is a directory");

    // Set up state.
    raf = new RandomAccessFile(file, "r");
    channel = raf.getChannel();
    remaining = file.length();

    // When bell rings, start a loop to send all chunks.
    new BellLoop(this) {
      public Bell lock() {
        return pause;
      } public boolean condition() {
        return remaining > 0;
      } public void body() throws Exception {
        long len = remaining < chunkSize ? remaining : chunkSize;
        len = channel.transferTo(offset, len, nioToFeather);
        offset += len;
        remaining -= len;
      } public void always() {
        finish();
      }
    }.start(bell);

    return bell;
  }

  protected void finish() {
    try {
      raf.close();
      channel.close();
    } catch (Exception e) { }
    super.finish();
  }
}

class LocalSink extends Sink<LocalResource> {
  final File file = destination().file();
  private RandomAccessFile raf;
  private FileChannel channel;
  private long offset = 0, remaining = 0;
  private long chunkSize = 4096;

  // State of the current transfer.
  public LocalSink(LocalResource root) { super(root); }

  public Bell start() {
    return new ThreadBell(destination().session.executor) {
      public Object run() throws Exception {
        if (file.exists()) {
          if (!file.canWrite())
            throw new RuntimeException("Permission denied");
          if (!file.isFile())
            throw new RuntimeException("Resource is a directory");
        }

        // Set up state.
        raf = new RandomAccessFile(file, "rw");
        channel = raf.getChannel();
        remaining = file.length();

        return null;
      }
    }.start();
  }

  public Bell drain(final Slice slice) {
    return new ThreadBell(destination().session.executor) {
      public Object run() throws Exception {
        channel.write(slice.asByteBuf().nioBuffer());
        slice.asByteBuf().release();
        return null;
      }
    }.start();
  }

  protected void finish() {
    try {
      raf.close();
      channel.close();
    } catch (Exception e) { }
  }
}
