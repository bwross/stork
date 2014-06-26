package stork.net;

import java.net.*;
import java.util.*;
import java.io.*;
import java.nio.file.*;

import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;
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
import stork.feather.*;
import stork.feather.util.*;
import stork.feather.URI;
import stork.feather.Path;
import stork.scheduler.*;

/**
 * A basic HTTP interface to tie the scheduler into the HTTP server.
 */
public class HTTPInterface extends StorkInterface {
  private final String host;
  private final int port;

  private static Map<URI, HTTPInterface> interfaces =
    new HashMap<URI, HTTPInterface>();

  public HTTPInterface(Scheduler s, URI uri) {
    super(s);

    host = (uri.host() != null) ? uri.host() : "localhost";
    port = uri.port();

    new HTTPServer.Route(uri, "GET", "POST") {
      public void handle(HTTPRequest request) {
        handleRequest(request);
      }
    };
  }

  public String name() { return "HTTP"; }

  public String address() {
    String a = host;
    if (port > 0) a += ":"+port;
    return a;
  }

  // Issue the request and relay the response to the client.
  private void handleRequest(final HTTPRequest request) {
    issueRequest(requestToAd(request)).new Promise() {
      public void done(Request sr) {
        sr.new As<Ad>() {
          public Ad convert(Object o) {
            return Ad.marshal(o);
          } public void done(Ad ad) {
            // Write the request back to the requestor.
            Pipes.tapFromString(ad).attach(request.root().sink());
          } public void fail(Throwable t) {
            //Taps.fromError(t).attach(request.root().sink());
          }
        };
      }
    };
  }

  // Convert an HTTP request to an ad asynchronously.
  private Bell<Ad> requestToAd(final HTTPRequest req) {
    Bell<Ad> bell = new Bell<Ad>();

    String command = req.uri.path().name();
    // Make sure the command exists and supports the method.
    Scheduler.Handler handler = handler(command);
    if (handler == null)
      return bell.ring(new HTTPException(404));
    if (!checkMethod(req.method(), handler))
      return bell.ring(new HTTPException(405));

    Ad ad = new Ad("command", command);

    // Use cookie as a base.
    if (req.cookie() != null)
      ad.addAll(cookiesToAd(req.cookie()));

    // Merge in query string.
    if (req.uri.query() != null)
      ad.addAll(queryToAd(req.uri.query()));

    // If there's no body, we're done. Else, parse asynchronously.
    if (!req.hasBody())
      bell.ring(ad);
    else
      handleRequestBody(ad, req).promise(bell);
    return bell;
  }

  // Asynchronously handle an HTTP request body.
  private Bell<Ad> handleRequestBody(final Ad head, HTTPRequest req) {
    String type = req.type();
    Pipes.AggregatorSink sink = Pipes.aggregatorSink();
    Bell<Ad> bell;

    // Make sure it's a type we can handle.
    if (type == null || type.startsWith("application/json")) {
      bell = sink.bell().new As<Ad>() {
        public Ad convert(Slice slice) {
          return Ad.parse(new ByteBufInputStream(slice.asByteBuf()));
        }
      };
    } else if (type.startsWith("application/x-www-form-urlencoded")) {
      bell = sink.bell().new As<Ad>() {
        public Ad convert(Slice slice) {
          return queryToAd(slice.asByteBuf().toString(CharsetUtil.UTF_8));
        }
      };
    } else {
      return new Bell<Ad>().ring(new Exception(type+" is unsupported."));
    }

    req.root().tap().attach(sink);
    return bell.new As<Ad>() {
      public Ad convert(Ad body) { return head.merge(body); }
    };
  }

  // Convert a cookie string into an ad.
  private Ad cookiesToAd(String cookie) {
    Ad ad = new Ad();
    Set<Cookie> cookies = CookieDecoder.decode(cookie);
    for (Cookie c : cookies)
      ad.put(c.getName(), c.getValue());
    return ad;
  }

  // Convert a query string into an ad.
  private Ad queryToAd(String query) {
    Ad ad = new Ad();
    QueryStringDecoder qsd = new QueryStringDecoder(query, false);
    Map<String, List<String>> map = qsd.parameters();
    for (String k : map.keySet())
      ad.put(k, map.get(k).get(0));
    return ad;
  }

  // Check that the handler allows the method.
  private boolean checkMethod(HttpMethod m, Scheduler.Handler h) {
    return m.equals(POST) || m.equals(GET) && !h.affectsState();
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

  public void init(Channel ch, ChannelPipeline pl) {
    pl.addLast(new HttpServerCodec());
    pl.addLast(new HttpContentCompressor());
    pl.addLast(new HTTPAdEncoder());
  }
}
