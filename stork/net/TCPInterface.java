package stork.net;

import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;

import java.net.*;

import stork.*;
import stork.ad.*;
import stork.feather.URI;
import stork.scheduler.*;

// Basic TCP interface.

public class TCPInterface extends StorkInterface {
  private URI uri;

  public TCPInterface(Scheduler s, URI uri) {
    super(s);
    this.uri = uri;
  }

  public String address() { return uri.host(); }

  public String name() { return "TCP"; }

  public void init(ServerChannel channel) {
    channel.pipeline().addLast(new AdDecoder());
  }

  public int port(URI uri) {
    return uri.port() > 0 ? uri.port() : 57024;
  }
}
