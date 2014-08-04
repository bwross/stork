package stork.module.ftp;

import java.net.*;
import java.util.*;
import java.nio.charset.*;

import io.netty.bootstrap.*;
import io.netty.channel.*;
import io.netty.buffer.*;
import io.netty.channel.nio.*;
import io.netty.channel.socket.*;
import io.netty.channel.socket.nio.*;
import io.netty.handler.codec.*;
import io.netty.handler.codec.string.*;
import io.netty.handler.codec.base64.*;
import io.netty.handler.codec.base64.Base64;
import io.netty.util.*;
import io.netty.util.concurrent.*;

import org.ietf.jgss.*;
import org.gridforum.jgss.*;

import stork.feather.*;
import stork.feather.URI;
import stork.util.*;

/**
 * An abstraction of an FTP control channel. This class takes care of command
 * pipelining, extracting replies, and maintaining channel state. Right now,
 * the channel is mostly concurrency-safe, though issues could arise if
 * arbitrary commands entered the pipeline during a command sequence. A
 * simplisitic channel locking mechanism exists which can be used for
 * synchronized access to the channel, but it is still ill-advised to have more
 * than one subsystem issuing commands through the same channel at the same
 * time.
 */
public class FTPChannel {
  // The maximum amount of time (in ms) to wait for a connection.
  static final int timeout = 2000;

  private final Bell<Void> onClose = new Bell<Void>();

  // FIXME: We should use something system-wide.
  static EventLoopGroup group = new NioEventLoopGroup(1);

  // Used for GSS authentication.
  private final String host;

  // Internal representation of the remote server type.
  private static enum Protocol {
    ftp(21), gridftp(2811);

    int port;

    Protocol(int def_port) { port = def_port; }
  }

  // Doing this allows state to more easily be shared between views of the same
  // channel.
  FTPSharedChannelState data;

  class FTPSharedChannelState {
    Protocol protocol;
    ChannelFuture future;
    Bell<SecurityContext> security;
    Deque<Command> handlers = new ArrayDeque<Command>();

    FTPChannel owner;  // The view that owns the underlying channel.

    // Any FTP server that adheres to specifications will use UTF-8, but let's
    // not preclude the possibility of being able to configure the encoding.
    Charset encoding = CharsetUtil.UTF_8;

    Reply welcome;
    FeatureSet features = new FeatureSet();

    Bell<Character> mode = new Bell<Character>('S');
    Bell<Character> type = new Bell<Character>('A');

    // Whether or not we prefer to put the server in passive mode.
    boolean preferPassive = true;
  }

  // Deferred commands
  Deque<Deferred> deferred = new ArrayDeque<Deferred>();

  public FTPChannel(String uri) {
    this(URI.create(uri));
  } public FTPChannel(URI uri) {
    this(uri.protocol(), uri.host(), null, uri.port());
  } public FTPChannel(String host, int port) {
    this(null, host, null, port);
  } public FTPChannel(InetAddress addr, int port) {
    this(null, null, addr, port);
  } public FTPChannel(String proto, String host, int port) {
    this(proto, host, null, port);
  } public FTPChannel(String proto, InetAddress addr, int port) {
    this(proto, null, addr, port);
  }

  // The above constructors delegate to this.
  private FTPChannel(String proto, String host, InetAddress addr, int port) {
    data = new FTPSharedChannelState();
    data.owner = this;

    this.host = (host != null) ? host : addr.toString();

    data.protocol = (proto == null) ?
      Protocol.ftp : Protocol.valueOf(proto.toLowerCase());
    if (port <= 0) port = data.protocol.port;

    Bootstrap b = new Bootstrap();
    b.group(group).channel(NioSocketChannel.class).handler(new Initializer());

    if (host != null)
      data.future = b.connect(host, port);
    else
      data.future = b.connect(addr, port);
  }

  // This special constructor is used internally to create channel views.
  // Calling this constructor will enqueue a synchronization command that will
  // allow the channel to assume control when it is reached.
  private FTPChannel(FTPChannel parent) {
    data = parent.data;
    host = parent.host;
  }

  // Handles initializing the control channel connection and attaching the
  // necessary codecs.
  class Initializer extends ChannelInitializer<SocketChannel> {
    public void initChannel(SocketChannel ch) throws Exception {
      ch.config().setConnectTimeoutMillis(timeout);

      ChannelPipeline p = ch.pipeline();

      p.addLast("line_decoder", new LineBasedFrameDecoder(2048));
      p.addLast("reply_decoder", new ReplyDecoder());
      p.addLast("reply_handler", new ReplyHandler());

      p.addLast("command_encoder", new CommandEncoder());

    }
  }

  // A reply from the server.
  public class Reply {
    public final int code;
    private final ByteBuf[] lines;

    private Reply(int code, ByteBuf[] lines) {
      if (code < 100 || code >= 700)
        throw new RuntimeException("Bad reply code: "+code);
      this.code = code;
      this.lines = lines;
    }

    // Get the number of lines.
    public int length() {
      return lines.length;
    }

    // Get a line by number.
    public String line(int i) {
      return lines[i].toString(data.encoding);
    }

    // Get an array of all the lines as strings.
    public String[] lines() {
      String[] arr = new String[lines.length];
      for (int i = 0; i < lines.length; i++)
        arr[i] = line(i);
      return arr;
    }

    // Return a description meaningful to a human based on a reply code.
    public String description() {
      return FTPMessages.fromCode(code);
    }

    // Return an exception based on the reply.
    public RuntimeException asError() {
      return new RuntimeException(description());
    }

    // Get the message as a string. If the reply is one line, the message is
    // the first line. If it's multi-lined, the message is the part between the
    // first and last lines.
    public String message() {
      if (lines.length <= 1)
        return line(0);
      StringBuilder sb = new StringBuilder();
      for (int i = 1, z = lines.length-1; i < z; i++) {
        sb.append(line(i));
        if (i != z) sb.append('\n');
      }
      return sb.toString();
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0, z = lines.length-1; i <= z; i++) if (i != z)
        sb.append(code).append('-').append(line(i)).append('\n');
      else
        sb.append(code).append(' ').append(line(i));
      return sb.toString();
    }

    // Returns true if this reply is intermediate and further replies are
    // expected before the command has been fulfilled.
    public boolean isPreliminary() {
      return code/100 == 1;
    }

    // Returns true if the reply indicates the command completed successfully.
    public boolean isComplete() {
      return code/100 == 2;
    }

    // Returns true if the reply indicates the command cannot be carried out
    // unless followed up with another command.
    public boolean isIncomplete() {
      return code/100 == 3;
    }

    // Returns true if the reply is negative (i.e., it indicates a failure).
    public boolean isNegative() {
      return code/100 == 4 || code/100 == 5;
    }

    // Returns true if the message is protected, i.e. its message is a payload
    // containing a specially encoded reply. In most cases, users will not see
    // replies of this type directly as the channel handlers will transparently
    // decode them. However, this can happen if server behaves improperly by
    // sending replies of this type with no security context set.
    public boolean isProtected() {
      return code/100 == 6;
    }
  }

  // Handles decoding replies from the server. This uses functionality from
  // LineBasedFrameDecoder to split incoming bytes on a per-line basis, then
  // parses each line as an FTP reply line, and buffers lines until a reply is
  // fully formed. It supports decoding (arbitrarily-nested) protected replies
  // when a security context is present.
  class ReplyDecoder extends ByteToMessageDecoder {
    List<ByteBuf> lines = new LinkedList<ByteBuf>();
    String codestr;
    int code;

    // This will receive whole lines as buffers.
    protected void decode(final ChannelHandlerContext ctx, 
                          ByteBuf buffer, final List out) {
      if (buffer != null) {
        Bell<Reply> r = asyncDecode(buffer);
        if (r != null) r.new Promise() {
          public void done(Reply r) {
            ctx.fireChannelRead(r);
          } public void fail(Throwable t) throws Exception {
            exceptionCaught(ctx, t);
          }
        };
      }
    }

    // Asynchronously decode and unprotect a buffer.
    protected Bell<Reply> asyncDecode(ByteBuf buffer) {
      if (buffer == null || buffer.readableBytes() <= 0)
        return null;  // We haven't gotten a full line.
      Reply r = decodeReplyLine(buffer);
      if (r == null)
        return null;  // We haven't gotten a full reply.
      return decodeProtectedReply(r);
    }

    // Convenience method for parsing reply text in a self-contained byte
    // buffer, used by decodeProtectedReply and anywhere else this might be
    // needed. This works by simulating incoming data in a channel pipeline,
    // though no channel context is present. This may be an issue if Netty ever
    // changes to use the channel context in line splitting, in which case we
    // will need to come up with another solution.
    private Bell<Reply> decodeProtectedReply(final Reply reply) {
      // Do nothing if the security context has not been initialized. Really
      // this is an error on the part of the server, but there's a chance the
      // client code will know what to do.
      if (data.security == null || !reply.isProtected())
        return new Bell<Reply>(reply);
      return data.security.new AsBell<Reply>() {
        public Bell<Reply> convert(SecurityContext sc) throws Exception {
          for (ByteBuf eb : reply.lines) {
            ByteBuf db = sc.unprotect(Base64.decode(eb));
            Bell<Reply> r = asyncDecode(db);
            if (r != null)
              return r;
          } throw new RuntimeException("Bad reply from server.");
        }
      };
    }

    // Decode a reply from a string. This should only be called if the input
    // buffer is a properly framed line with the end-of-line character(s)
    // stripped.
    protected Reply decodeReplyLine(ByteBuf msg) {
      try {
        return innerDecodeReplyLine(msg);
      } catch (Exception e) {
        // TODO: Replace this with a real exception.
        throw new RuntimeException("Bad reply from server.", e);
      }
    } protected Reply innerDecodeReplyLine(ByteBuf msg) throws Exception {
      // Some implementation supposedly inserts null bytes, ugh.
      if (msg.getByte(0) == 0)
        msg.readByte();

      char sep = '-';

      // Extract the reply code and separator.
      if (lines.isEmpty()) {
        codestr = msg.toString(0, 3, data.encoding);
        code = Integer.parseInt(codestr);
        msg.skipBytes(3);
        sep = (char) msg.readByte();
      } else if (msg.readableBytes() >= 4) {
        String s = msg.toString(0, 4, data.encoding);
        sep = s.charAt(3);
        if (s.startsWith(codestr) && (sep == '-' || sep == ' '))
          msg.skipBytes(4);
        else
          sep = '-';
      }

      // Save the rest of the message.
      lines.add(msg.copy());
      msg.skipBytes(msg.readableBytes());

      // Act based on the separator.
      switch (sep) {
        case ' ':
          ByteBuf[] la = lines.toArray(new ByteBuf[lines.size()]);
          lines = new LinkedList<ByteBuf>();
          return new Reply(code, la);
        case '-': return null;
        default : throw new RuntimeException("Unexpected: "+sep);
      }
    }
  }

  // Handles replies as they are received.
  class ReplyHandler extends SimpleChannelInboundHandler<Reply> {
    public boolean acceptInboundMessage(Object msg) {
      return msg instanceof Reply;
    }

    public void messageReceived(ChannelHandlerContext ctx, Reply reply) {
      //Log.finer("Got: ", reply);
      switch (reply.code) {
        case 220:
          if (data.welcome == null)
            data.welcome = reply;
          break;
        case 421:
          FTPChannel.this.close();
          break;
        default:
          feedHandler(reply);
      }
    }

    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
      FTPChannel.this.close();
    }

    // TODO: How should we handle exceptions? Which exceptions can this thing
    // receive anyway?
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
      FTPChannel.this.close();
    }
  }

  // A security context represents all of the state of the channel with respect
  // to security and provides methods for encoding and decoding messages.
  interface SecurityContext {
    // Returns true if the session has been established.
    boolean established();

    // Given an input token, return an output token which should be given to
    // the remote server via ADAT, or null if the session is established.
    ByteBuf handshake(ByteBuf in) throws Exception;

    // Given a byte buffer containing protected bytes (not a Base64 encoding
    // thereof), decode the bytes back into the plaintext payload.
    ByteBuf unprotect(ByteBuf buf) throws Exception;

    // Given a byte buffer containing some information, generate a command with
    // a payload protected according to the current security context.
    ByteBuf protect(ByteBuf buf) throws Exception;
  }

  // A security context based on GSSAPI.
  class GSSSecurityContext implements SecurityContext {
    GSSContext context;

    public GSSSecurityContext(GSSCredential cred) throws GSSException {
      GSSManager manager = ExtendedGSSManager.getInstance();
      Oid oid = new Oid("1.3.6.1.4.1.3536.1.1");
      GSSName peer = manager.createName(
        "host@"+host, GSSName.NT_HOSTBASED_SERVICE);
      context = manager.createContext(
        peer, oid, cred, cred.getRemainingLifetime());
      context.requestCredDeleg(true);
      context.requestConf(true);
    }

    // Utility method for extracting a buffer as a byte array.
    private byte[] bytes(ByteBuf buf) {
      byte[] b;
      if (buf.hasArray())
        b = buf.array();
      else
        buf.getBytes(buf.readerIndex(), b = new byte[buf.readableBytes()]);
      return b;
    }

    public boolean established() {
      return context.isEstablished();
    }

    public ByteBuf handshake(ByteBuf in) throws GSSException {
      if (established())
        return null;
      byte[] i = bytes(in);
      byte[] o = context.initSecContext(i, 0, i.length);
      return Unpooled.wrappedBuffer(o);
    }

    public ByteBuf unprotect(ByteBuf buf) throws GSSException {
      byte[] b = bytes(buf);
      return Unpooled.wrappedBuffer(context.unwrap(b, 0, b.length, null));
    }

    public ByteBuf protect(ByteBuf buf) throws GSSException {
      byte[] b = bytes(buf);
      return Unpooled.wrappedBuffer(context.wrap(b, 0, b.length, null));
    }
  }

  // Handles encoding commands using the current charset and security
  // mechanism. We can assume every message is a single command with no
  // newlines.
  class CommandEncoder extends MessageToMessageEncoder<Object> {
    protected void encode
      (final ChannelHandlerContext ctx, Object msg, final List<Object> out)
    throws Exception {
      //Log.finer("Writing ",msg);
      final ByteBuf raw =
        Unpooled.wrappedBuffer(msg.toString().getBytes(data.encoding));

      if (data.security == null) {
        out.add(raw);
        out.add(Unpooled.wrappedBuffer("\r\n".getBytes(data.encoding)));
      } else data.security.new As<ByteBuf>() {
        public ByteBuf convert(SecurityContext sc) throws Exception {
          return Unpooled.wrappedBuffer(
            Unpooled.wrappedBuffer("ENC ".getBytes(data.encoding)),
            Base64.encode(sc.protect(raw), false));
        } public void done(ByteBuf encoded) {
          out.add(encoded);
        } public void fail() {
          out.add(raw);
        } public void always() {
          out.add(Unpooled.wrappedBuffer("\r\n".getBytes(data.encoding)));
        }
      }; 
    }
  }

  // Used internally to extract the channel from the future.
  private Channel channel() {
    return data.future.syncUninterruptibly().channel();
  }

  // Close the channel and run the onClose handler.
  public synchronized void close() {
    if (!isClosed()) {
      channel().close();
      onClose.ring();
    }
  }

  // Check if the channel has been closed.
  public synchronized boolean isClosed() {
    return onClose.isDone();
  }

  // This gets called when the channel is closed by some means. Subclasses
  // should implement this.
  public Bell<?> onClose() {
    return onClose;
  }

  // Try to authenticate using a username and password.
  public Bell<Reply> authorize() {
    return authorize("anonymous", "");
  } public Bell<Reply> authorize(String user) {
    return authorize(user, "");
  } public Bell<Reply> authorize(final String user, final String pass) {
    return new Command("USER", user).new AsBell<Reply>() {
      public Bell<Reply> convert(Reply r) {
        if (r.code == 331)
          return new Command("PASS", pass);
        else if (r.isComplete())
          ring(r);
        else
          ring(r.asError());
        return null;
      }
    };
  }

  // Try to authenticate with a GSS credential.
  public Bell<Reply> authenticate(GSSCredential cred) {
    final GSSSecurityContext sec;

    if (data.security != null)
      throw new RuntimeException("already authenticated");

    data.security = new Bell<SecurityContext>();

    try {
      sec = new GSSSecurityContext(cred);
    } catch (Exception e) {
      return new Bell<Reply>().ring(e);
    }

    return new Command("AUTH GSSAPI").new Promise() {
      public void then(Reply r) {
        if (r.isNegative()) {
          ring(r.asError());
        } else if (r.isIncomplete()) {
          handshake(sec, Unpooled.EMPTY_BUFFER).promise(this);
        } else {
          data.security.ring(sec); ring(r);
        }
      }
    };
  }

  // Handshake procedure for all authentication types. The input byte buffer
  // should be the raw binary token, not a Base64 encoding.
  private Bell<Reply> handshake(final SecurityContext sec, ByteBuf it) {
    final Bell<Reply> bell = new Bell<Reply>();

    try {
      ByteBuf ot = Base64.encode(sec.handshake(it), false);

      new Command("ADAT", ot.toString(data.encoding)) {
        public void done(Reply r) throws Exception {
          switch (r.code/100) {
            case 3:
              ByteBuf token = Base64.decode(r.lines[0].skipBytes(5));
              handshake(sec, token).promise(bell);
            case 2:
              promise(bell); return;
            default:
              throw r.asError();
          }
        }
      };
    } catch (Exception e) {
      bell.ring(e);
    }

    return bell;
  }

  // A structure used to hold a set of features supported by a server. A fine
  // specimen of overengineering.
  // TODO: This should probably propagate control channel errors.
  class FeatureSet {
    private Map<String,Bell> features;

    // Asynchronously check if a command is supported.
    private synchronized void init() {
      if (features != null)
        return;

      features = new HashMap<String,Bell>();

      // ...and pipe all the check commands.
      new Command("HELP") {
        public void done(Reply r) {
          if (!r.isComplete()) return;
          for (int i = 1; i < r.length()-1; i++)
          for (String c : r.line(i).trim().toUpperCase().split(" "))
            if (!c.contains("*")) addFeature(c);
        }
      };
      new Command("HELP SITE") {
        public void done(Reply r) {
          if (!r.isComplete()) return;
          for (int i = 1; i < r.length()-1; i++)
            addFeature(r.line(i).trim().split(" ", 2)[0].toUpperCase());
        }
      };
      //new Command("FEAT");  // TODO
      new Command(null) {
        public void done() { finalizeChecks(); }
      };
    }

    public synchronized Bell supports(String cmd) {
      init();
      Bell bell = features.get(cmd);
      if (bell == null)
        features.put(cmd, bell = new Bell());
      return bell;
    }

    private synchronized void addFeature(String cmd) {
      init();
      Bell bell = features.get(cmd);
      if (bell == null)
        features.put(cmd, bell = new Bell());
      bell.ring();
    }

    // Ring this when no more commands will be issued to check support. It will
    // resolve all unsupported commands with false and update the state to
    // reflect the command list has been finalized.
    private synchronized void finalizeChecks() {
      for (Bell b : features.values())
        b.ring(new RuntimeException());
    }
  }

  // Checks if a command is supported by the server.
  public Bell<Boolean> supports(String cmd) {
    return data.features.supports(cmd).as(true, false);
  } private Bell[] supportsMulti(String... cmd) {
    Bell[] bells = new Bell[cmd.length];
    for (int i = 0; i < cmd.length; i++)
      bells[i] = data.features.supports(cmd[i]);
    return bells;
  } public Bell<Boolean> supportsAny(String... cmd) {
    return Bell.any(supportsMulti(cmd)).as(true, false);
  } public Bell<Boolean> supportsAll(String... cmd) {
    return Bell.all(supportsMulti(cmd)).as(true, false);
  }

  // Append the given command to the handler queue. If the command is a sync
  // command and there is nothing else in the queue, just fulfill it
  // immediately and don't put it in the queue. This will return whether the
  // command handler was appended or not (currently, it is always true).
  private boolean appendHandler(Command c) {
    synchronized (data.handlers) {
      if (!c.isSync() || !data.handlers.isEmpty())
        return data.handlers.add(c);
    }

    // If we didn't return, it's a sync. Handling a sync is fast, but we should
    // release handlers as soon as possible.
    c.ring();
    return true;
  }

  // "Feed" the topmost handler a reply. If the reply is a preliminary reply,
  // it will pop the command handler and any sync commands between it and the
  // next non-sync command (or the end of the queue). In order to release the
  // monitor on the queue as soon as possible, the command handlers are not
  // called until after the queue has been modified.
  private void feedHandler(Reply reply) {
    Command handler;
    List<Command> syncs = null;

    // We should try to release this as soon as possibile, so don't do anything
    // that might take a long time in here.
    synchronized (data.handlers) {
      assert !data.handlers.isEmpty();

      if (reply.isPreliminary()) {
        handler = data.handlers.peek();
      } else {
        handler = data.handlers.pop();

        // Remove all the syncs.
        Command peek = data.handlers.peek();
        if (peek != null && peek.isSync()) {
          syncs = new LinkedList<Command>();
          do {
            syncs.add(peek);
            data.handlers.pop();
            peek = data.handlers.peek();
          } while (peek != null && peek.isSync());
        }
      }
    }

    // Now we can call the handlers.
    if (reply.isPreliminary())
      handler.handle(reply);
    else
      handler.ring(reply);
    if (syncs != null) for (Command sync : syncs)
      sync.ring();
  }

  /**
   * Change the channel data type.
   * @return The data type after this command.
   */
  public synchronized Bell<Character> type(char t) {
    return data.type =
      new Command("TYPE", t).expectComplete().as(t).or(data.type);
  }

  /**
   * Change the channel transfer mode.
   * @return The transfer mode after this command.
   */
  public synchronized Bell<Character> mode(char m) {
    return data.mode =
      new Command("MODE", m).expectComplete().as(m).or(data.mode);
  }

  /** Negotiate a passive mode data channel. */
  public synchronized Bell<FTPHostPort> passive() {
    return new Command("PASV").expectComplete().new As<FTPHostPort>() {
      public FTPHostPort convert(Reply r) { return new FTPHostPort(r); }
    };
  }

  /**
   * Instantiating this class requests a lock on the channel and returns a
   * special view of the channel once the lock request is satisfied. The
   * special channel view will have exclusive use of the channel until unlocked
   * (after which the channel lock will become unusable) or garbage collected.
   * The channel should be unlocked as soon as the command sequence requiring
   * synchronicity has been issued. Commands written to any other channels
   * during this time will have their commands deferred and sent after the
   * channel has been unlocked.
   */
  public class Lock extends FTPChannel {
    public Lock() {
      super(FTPChannel.this);
      FTPChannel.this.new Command(null) {
        public void done() { Lock.this.assumeControl(); }
      };
    } public Bell<FTPChannel> lock() {
      throw new IllegalStateException("cannot lock from a view");
    } public void unlock() {
      new Command(null) {
        public void done() { FTPChannel.this.assumeControl(); }
      };
    } protected void finalize() {
      if (data.owner == this)
        FTPChannel.this.assumeControl();
    }
  }

  // This is called whenever it's the the channel's turn to become the channel
  // owner.
  protected synchronized void assumeControl() {
    synchronized (data) {
      //Log.finer(data.owner.hashCode()+" -> "+hashCode());
      data.owner = this;
      if (!deferred.isEmpty()) {
        Deque<Deferred> realDeferred = deferred;
        deferred = new LinkedList<Deferred>();

        for (Deferred d : realDeferred) d.send();

        realDeferred.clear();
        realDeferred.addAll(deferred);
        deferred = realDeferred;
      }
    }
  }

  // Release the lock, if this channel is a locked view. If it's not (and the
  // base channel never is), this throws an exception.
  public void unlock() {
    throw new IllegalStateException("channel is not locked");
  }

  // A deferred command which holds onto its arguments until it is ready to be
  // written. Once it's ready to be written, call send(). If the view is not
  // the owner when send() is called, the command will once again be deferred.
  private class Deferred {
    final Command cmd;
    final Object verb;
    final Object[] args;

    Deferred(Command c, Object v, Object[] a) {
      cmd = c; verb = v; args = a;
    } void send() {
      // If we're not the owner, defer the command. Otherwise, send it.
      if (data.owner != FTPChannel.this) {
        deferred.add(this);
        //Log.finer(FTPChannel.this.hashCode()+": Deferring "+this);
      } else {
        appendHandler(cmd);
        if (verb != null) channel().writeAndFlush(this);
        //Log.finer(FTPChannel.this.hashCode()+": Sending "+this);
      }
    } public String toString() {
      if (verb == null)
        return "(sync)";
      StringBuilder sb = new StringBuilder(verb.toString());
      for (Object o : args)
        sb.append(" ").append(o);
      return sb.toString();
    }
  }

  /**
   * This is sort of the workhorse of the channel. Instantiate one of these to
   * send a command across this channel. The newly instantiated object will
   * serve as a "future" for the server's ultimate reply. For example, a simple
   * authorization routine could be written like:
   *
   *   FTPChannel ch = new FTPChannel(...);
   *   Reply r = ch.new Command("USER bob") {
   *     public void done(Reply r) {
   *       if (r.code == 331)
   *         new Command("PASS monkey");
   *     }
   *   }.sync();
   *
   * This class's constructor will cause the command to be written to the
   * channel and placed in the handler queue. This class can be anonymously
   * subclassed to write command handlers inline, or can be subclassed normally
   * for repeat issue of the command.
   */
  public class Command extends Bell<Reply> {
    private final boolean isSync;

    /**
     * Constructing this will automatically cause the given command to be
     * written to the server. Typically, the passed cmd will be a string, but
     * can be anything. The passed arguments will be stringified and
     * concatenated with spaces for convenience. If {@code verb} is {@code
     * null}, nothing is actually written to the server, and the command bell
     * will ring when all commands piped prior to the sync have completed.
     */
    public Command(Object verb, Object... args) {
      synchronized (data) {
        isSync = (verb == null);
        new Deferred(this, verb, args).send();
      }
    } 

    /** An optional handler for intermediate replies. */
    public void handle(Reply r) { }

    /**
     * Return a bell which will fail if the reply code is not in the given
     * range (inclusive), instead of ringing successfully with the reply.
     */
    public Bell<Reply> expect(final int lo, final int hi) {
      return this.new Promise() {
        public void then(Reply r) {
          if (r.code < lo || r.code > hi)
            throw r.asError();
          ring(r);
        }
      };
    }

    /**
     * Return a bell which will fail if the reply code does not match the given
     * code, instead of ringing successfully with the reply.
     */
    public Bell<Reply> expect(int code)   { return expect(code, code); }

    public Bell<Reply> expectComplete()   { return expect(200, 299); }

    public Bell<Reply> expectIncomplete() { return expect(300, 399); }

    public Bell<Reply> expectNegative()   { return expect(400, 599); }

    public synchronized final boolean isSync() { return isSync; }
  }

  /**
   * Asynchronous FTP data channel abstraction. Subclasses must override {@link
   * #receive(Slice)} to handle incoming data. This channel extends {@code
   * Lock}, but handles its own unlocking.
   */
  public class DataChannel extends Lock {
    private Bell<SocketChannel> dc;
    private volatile boolean read = false;
    private ChannelHandlerContext context;
    private Bell writeBell;  // Ring when we can write again.
    private Bell lastSend = Bell.rungBell();

    // Ring this to close the channel.
    private final Bell<DataChannel> onClose = new Bell<DataChannel>() {
      public void always() {
        dc.cancel().new Promise() {
          public void done(SocketChannel ch) { ch.close(); }
        };
      }
    };

    public DataChannel() {
      this(FTPChannel.this.data.preferPassive);
    }

    public DataChannel(boolean preferPassive) {
      FTPChannel.this.super();
      dc = preferPassive ? tryPassiveThenActive() : tryActiveThenPassive();
      dc.new AsBell<SocketChannel>() {
        public Bell<SocketChannel> convert(SocketChannel c) {
          return init().as(c);
        } public void fail(Throwable t) {
          close(t);
        } public void always() {
          DataChannel.super.unlock();
        }
      };
    }

    private Bell<SocketChannel> tryPassiveThenActive() {
      return tryPassive().new Promise() {
        public void then(Throwable t) { tryActive().promise(this); }
      };
    }

    private Bell<SocketChannel> tryActiveThenPassive() {
      return tryActive().new Promise() {
        public void then(Throwable t) { tryPassive().promise(this); }
      };
    }

    private Bell<SocketChannel> tryPassive() {
      return passive().new AsBell<SocketChannel>() {
        public Bell<SocketChannel> convert(FTPHostPort hp) {
          Bootstrap b = new Bootstrap();
          b.group(FTPChannel.group).channel(NioSocketChannel.class);
          b.handler(new ChannelInitializer<SocketChannel>() {
            public void initChannel(SocketChannel ch) throws Exception {
              ch.config().setConnectTimeoutMillis(timeout);
              ch.pipeline().addLast(new SliceHandler());
            }
          });
          return futureToBell(b.connect(hp.getAddr()));
        }
      };
    }

    private Bell<SocketChannel> tryActive() {
      return new Bell<SocketChannel>(new RuntimeException("TODO"));
    }

    // Make a future into a bell that reverse cancels.
    private Bell<SocketChannel> futureToBell(final ChannelFuture cf) {
      return new Bell<SocketChannel>() {
        {
          cf.addListener(new GenericFutureListener<ChannelFuture>() {
            public void operationComplete(ChannelFuture f) {
              try {
                ring((SocketChannel) f.channel());
              } catch (Exception e) {
                ring(e);
              }
            }
          });
        } public void fail(Throwable t) {
          cf.cancel(true);
        }
      };
    }

    // Handle incoming data chunks and forward to handler.
    // TODO: Mode E, encryption, pause/resume.
    class SliceHandler extends ChannelHandlerAdapter {
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        receive(new Slice((ByteBuf) msg));
      } public void channelInactive(ChannelHandlerContext ctx) {
        DataChannel.this.close();
      } public void read(ChannelHandlerContext ctx) {
        synchronized (FTPChannel.this) {
          if (read)
            ctx.read();
          else if (context == null)
            context = ctx;
        }
      } public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        System.out.println(ctx.channel().isWritable());
        writable(ctx.channel().isWritable());
      }
    }

    /** Called to change writability. */
    private synchronized void writable(boolean writable) {
      if (!writable && writeBell == null) {
        writeBell = new Bell();
      } else if (writable && writeBell != null) {
        writeBell.ring();
        writeBell = null;
      }
    }

    /** This must not be called externally. */
    public void unlock() {
      throw new Error("do not call unlock() on a DataChannel");
    }

    /** Return a bell which rings on connect. */
    public Bell<DataChannel> onConnect() {
      return dc.as(this);
    }

    /** Return a bell which rings on close (or failure). */
    public Bell<DataChannel> onClose() {
      return onClose;
    }

    /** Start reading data when {@code bell} rings. */
    public synchronized DataChannel startWhen(Bell bell) {
      bell.new Promise() {
        public void done() { start(); }
        public void fail() { close(); }
      };
      return this;
    }

    /** Pause reading until {@code bell} rings. */
    public synchronized DataChannel pauseUntil(Bell bell) {
      if (bell != null && !bell.isDone()) {
        stop();
        startWhen(bell);
      } return this;
    }

    /** Start reading data. */
    public synchronized void start() {
      read = true;
      if (context != null)
        context.read();
      context = null;
    }

    /** Stop reading data. */
    public synchronized void stop() {
      read = false;
    }

    /** Pipe commands to be run in the lock. */
    public Bell init() { return Bell.rungBell(); }

    /** Close the channel. */
    public final void close() { onClose.ring(this); }

    /** Close the channel with a failure. */
    public final void close(Throwable t) { onClose.ring(t); }

    /** Subclasses use this to handle slices. */
    public void receive(Slice slice) { }

    /** Send a slice through the data channel. */
    public synchronized Bell send(final Slice slice) {
      return lastSend = dc.new Promise() {
        public void done(SocketChannel ch) {
          ch.writeAndFlush(slice.asByteBuf());
        }
      }.and(writeBell);
    }
  }

  public static void main(String[] args) throws Exception {
    FTPChannel ch = new FTPChannel(args[1]);
    java.io.BufferedReader r = new java.io.BufferedReader(
      new java.io.InputStreamReader(System.in));
    while (true)
      ch.new Command(r.readLine());
  }
}
