package stork.net;

import io.netty.handler.codec.http.*;
import io.netty.buffer.*;

import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

import stork.feather.*;

// The session used by HTTPBody.
public abstract class HTTPRequest extends Session<HTTPRequest,HTTPBody> {
  final HttpRequest netty;

  public boolean ready = false;

  public HTTPRequest(HttpRequest netty) {
    super(URI.create(netty.getUri()));
    this.netty = netty;

    if (isMultipart())
      throw new RuntimeException("multipart is currently unsupported");
  }

  public HTTPBody select(Path path) {
    return new HTTPBody(this, path);
  }

  /** Take care of sending data through the right {@code HTTPBody}. */
  public void translate(ByteBuf buffer) {
  }

  public boolean isMultipart() {
    String t = header(CONTENT_TYPE);
    return t != null && t.startsWith("multipart/");
  }

  /** Check for a header. */
  public String header(CharSequence name) {
    return netty.headers().get(name);
  }

  /** Send something back to Netty. */
  public abstract Bell toNetty(HttpObject obj);

  /** Total size of the request. */
  public long size() {
    try {
      return Long.parseLong(header(CONTENT_LENGTH));
    } catch (Exception e) {
      return -1;
    }
  }

  public boolean hasBody() {
    return size() > 0;
  }

  public HttpVersion version() {
    return netty.getProtocolVersion();
  }

  public HttpMethod method() {
    return netty.getMethod();
  }

  public String cookie() { return header(COOKIE); }

  public String type() { return header(CONTENT_TYPE); }

  /** Close the transfer. */
  public void finalize() {

  }
}
