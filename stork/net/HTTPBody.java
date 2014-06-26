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
import stork.feather.URI;
import stork.feather.Path;
import stork.scheduler.*;

/**
 * A representation of an incoming HTTP request. This object provides accessors
 * for request metadata, as well as a tap/sink-based abstraction layer to the
 * Netty backend.
 */
public class HTTPBody extends Resource<HTTPRequest,HTTPBody> {
  protected URI uri;

  /** Create an HTTPBody from the given HTTP request. */
  public HTTPBody(HTTPRequest req) {
    super(req);
  }

  /** Create an HTTPBody representing a body part. */
  protected HTTPBody(HTTPRequest req, Path path) {
    super(req, path);
  }

  /**
   * This will emit the content body as the data for the root resource.
   */
  private class HTTPTap extends Tap<HTTPBody> {
    HTTPTap() {
      super(HTTPBody.this);
    }

    public Bell start(Bell bell) {
      return bell.new Promise() {
        public void done() {
          session.ready = true;
        } public void fail() {
          // TODO
        }
      };
    }
  };

  /**
   * This will proxy data to the client. Data resources should be sent directly
   * to the client in the response body. TODO: Collection resources need to be
   * zipped or sent using some similar archival encoding. We can't use
   * multipart responses because not all browsers support it.
   */
  private class HTTPSink extends Sink<HTTPBody> {
    HTTPSink() { super(HTTPBody.this); }

    // If this is the root, send a header through Netty.
    public Bell start() {
      if (!source().path.isRoot())  // Sigh browsers...
        throw new RuntimeException("Cannot send multiple files.");
      return generateHeader(destination()).new As<HTTPBody>() {
        public HTTPBody convert(HttpResponse o) {
          session.toNetty(o);
          return HTTPBody.this;
        } public HTTPBody convert(Throwable t) throws Throwable {
          session.toNetty(errorToHttpMessage(t));
          throw t;
        }
      };
    }

    public Bell drain(Slice slice) {
      return session.toNetty(new DefaultHttpContent(slice.asByteBuf()));
    }

    public void finish() {
      session.toNetty(new DefaultLastHttpContent());
    }
  };

  /**
   * Generate an HTTP header for another resource based on its metadata.
   */
  private Bell<HttpResponse> generateHeader(Resource<?,?> resource) {
    return resource.stat().new As<HttpResponse>() {
      public HttpResponse convert(Stat stat) {
        if (stat.dir)
          throw new RuntimeException("Cannot send directories.");
        if (stat.size == 0)
          return new DefaultFullHttpResponse(session.version(), NO_CONTENT);
        HttpResponse r = new DefaultHttpResponse(session.version(), OK);
        r.headers().set(CONTENT_LENGTH, stat.size);
        r.headers().set(CONTENT_TYPE, "application/octet-stream");
        return r;
      }
    };
  }

  /**
   * Create an HTTP response for a throwable.
   */
  private HttpMessage errorToHttpMessage(Throwable t) {
    String message = StorkInterface.errorToAd(t).get("message");
    ByteBuf b = Unpooled.copiedBuffer(message.getBytes());
    FullHttpResponse r = new DefaultFullHttpResponse(
      HTTP_1_1, INTERNAL_SERVER_ERROR, b);
    r.headers().set(CONTENT_TYPE, "text/plain");
    return r;
  }

  /** Return true if the tap can emit. */
  public boolean ready() {
    return session.ready;
  }
  
  /**
   * Get the length from the Content-Length header.
   */
  public long size() {
    try {
      return Long.parseLong(session.header("Content-Length"));
    } catch (Exception e) {
      return -1;
    }
  }

  public Bell<Stat> stat() {
    if (isRoot()) {
      Stat s = new Stat();
      s.name = "/";
      s.size = size();
      s.dir = session.isMultipart();
      s.file = !s.dir;
      return new Bell<Stat>().ring(s);
    } else {
      return null;  // TODO
    }
  }

  /** This will emit data coming from the client. */
  public Tap<HTTPBody> tap() {
    return new HTTPTap();
  }

  /** This will send data to the client. */
  public Sink<HTTPBody> sink() {
    return new HTTPSink();
  }
}
