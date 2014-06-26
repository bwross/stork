package stork.net;

import java.net.*;
import java.util.*;
import java.io.*;
import java.nio.file.*;

import io.netty.bootstrap.*;
import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;
import io.netty.channel.socket.nio.*;
import io.netty.channel.nio.*;
import io.netty.handler.codec.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.*;
import io.netty.handler.stream.*;
import io.netty.util.*;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static io.netty.handler.codec.http.HttpMethod.*;

import stork.ad.*;
import stork.feather.Bell;
import stork.feather.URI;
import stork.feather.Path;
import stork.scheduler.*;

/**
 * A simple prefix routing HTTP server utility integrated with Feather. This
 * was created primarily to simplify developer deployments and allow greater
 * flexibility in the REST interface.
 * <p/>
 * Note that this class is for the internal Stork HTTP server for handling
 * incoming client connections. It is not related to the HTTP transfer module,
 * and should not be used directly by any module code.
 */
public class HTTPServer {
  private static Map<InetSocketAddress, HTTPServer> servers =
    new HashMap<InetSocketAddress, HTTPServer>();

  // Map method and path to route handler.
  private Map<HttpMethod,Map<Path,Route>> routes =
    new HashMap<HttpMethod,Map<Path,Route>>();

  /**
   * Return an {@code HTTPServer} bound to the given host and port. If an
   * {@code HTTPServer} has already been instantiated bound to an equivalent
   * address, it will be returned.
   */
  public static synchronized HTTPServer create(String host, int port) {
    InetSocketAddress isa = new InetSocketAddress(host, port);
    HTTPServer server = servers.get(isa);
    if (server == null)
      servers.put(isa, server = new HTTPServer(isa));
    return server;
  }

  // Create and bind an HTTP server to the given socket address.
  private HTTPServer(InetSocketAddress isa) {
    ServerBootstrap sb = new ServerBootstrap();
    sb.channel(NioServerSocketChannel.class);
    sb.group(new NioEventLoopGroup());
    sb.childHandler(new ChannelInitializer<ServerSocketChannel>() {
      protected void initChannel(ServerSocketChannel ch) {
        ChannelPipeline pl = ch.pipeline();
        pl.addLast(new HttpServerCodec());
        pl.addLast(new RequestHandler());
      }
    });

    sb.option(ChannelOption.TCP_NODELAY, true);
    sb.option(ChannelOption.SO_KEEPALIVE, true);

    sb.bind(isa);
  }

  private synchronized void addRoute(String[] methods, Route route) {
    if (methods == null || methods.length == 0) {
      methods = new String[] { null };
    } for (String ms : methods) {
      HttpMethod m = HttpMethod.valueOf(ms);
      Map<Path,Route> rm = routes.get(m);
      if (rm == null)
        routes.put(m, rm = new HashMap<Path,Route>());
      rm.put(route.prefix, route);
    }
  }

  /**
   * Instantiate a {@code Route} to add a route handler.
   */
  public static abstract class Route {
    public final Path prefix;

    /**
     * Create a routing table entry for the given methods and prefix.
     *
     * @param uri the URI to match this route against.
     * @param method the methods to match this route with; if empty or {@code
     * null}, this route is matched against any method.
     */
    public Route(URI uri, String... method) {
      this.prefix = (uri.path() == null) ? Path.ROOT : uri.path();
      create(uri.host(), port(uri)).addRoute(method, this);
    }

    /**
     * Subclasses should implement this to handle a request.
     */
    public abstract void handle(HTTPRequest request);
  }

  /**
   * Returns the first matched route. This first checks for a route for the
   * given method and path. If a route cannot be found, it checks for a route
   * for the wildcard (null) method and the path. If a route still cannot be
   * found, it repeats the procedure for the parent of the path. This is done
   * until a route is found or the path has no parent.
   */
  private Route route(HttpMethod method, Path path) {
    Map<Path,Route> mm = routes.get(method);
    Map<Path,Route> nm = routes.get(null);

    if (mm == null && nm == null) {
      return null;
    } while (true) {
      if (mm != null && mm.containsKey(path))
        return mm.get(path);
      if (nm != null && nm.containsKey(path))
        return mm.get(path);
      if (!path.isRoot())
        path = path.up();
      else
        return null;
    }
  }

  /**
   * A channel handler for incoming HTTP requests which ties Netty to Feather.
   */
  private class RequestHandler extends ChannelHandlerAdapter {
    private HTTPRequest request;
    private Bell pauseBell;

    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      if (msg instanceof HttpObject)
        channelRead(ctx, (HttpObject) msg);
      else
        ctx.close();
    }

    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
      if (!ctx.channel().isWritable()) {
        if (pauseBell == null) pauseBell = new Bell();
      } else if (pauseBell != null) {
        pauseBell.ring();
        pauseBell = null;
      }
    }

    public void channelRead(final ChannelHandlerContext ctx, HttpObject msg) {
      boolean done = false;

      // Handle routing and wrapping request headers.
      if (msg instanceof HttpRequest) {
        HttpRequest head = (HttpRequest) msg;
        URI uri = URI.create(head.getUri());

        // Run the requested path through the router.
        Route route = route(head.getMethod(), uri.path());
        if (route == null)
          throw new HTTPException(NOT_FOUND);

        request = new HTTPRequest(head) {
          public Bell toNetty(HttpObject o) {
            ctx.write(o);
            return pauseBell;
          }
        };

        done = (request.size() <= 0);
      }

      // Handle request content.
      if (msg instanceof HttpContent) {
        HttpContent c = (HttpContent) msg;
        request.translate(c.content());

        done = done || (msg instanceof LastHttpContent);
      }

      // Finalize the request and prepare for next request.
      if (done) {
        request.close();
        request = null;
      }
    }

    public void read(ChannelHandlerContext ctx) {
      if (request == null || request.ready)
        ctx.read();
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
      t = t.getCause();
      t.printStackTrace();
      HTTPException he = (t instanceof HTTPException) ?
        (HTTPException) t : new HTTPException(INTERNAL_SERVER_ERROR);
      ctx.writeAndFlush(he.toHttpMessage());
      ctx.close();
    }
  }

  // A codec for converting ads to HTTP responses.
  private class HTTPAdEncoder extends MessageToMessageEncoder<Ad> {
    public void encode(ChannelHandlerContext ctx, Ad ad, List<Object> out) {
      String error = ad.get("error");
      FullHttpResponse r;
      if (error == null) {
        ByteBuf b = Unpooled.copiedBuffer(ad.toJSON().getBytes());
        r = new DefaultFullHttpResponse(HTTP_1_1, OK, b);
        r.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
      } else {
        ByteBuf b = Unpooled.copiedBuffer(error.getBytes());
        r = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR, b);
        r.headers().set(CONTENT_TYPE, "text/plain");
      } out.add(r);
    }
  }

  public static int port(URI uri) {
    if (uri.port() > 0)
      return uri.port();
    return "https".equals(uri.scheme()) ? 443 : 80;
  }
}
