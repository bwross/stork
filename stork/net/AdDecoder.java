package stork.net;

import java.io.*;
import java.util.*;

import stork.ad.*;

import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.channel.*;

/**
 * A decoder that passes character stream data through an {@code AdParser}.
 */
public class AdDecoder extends ReplayingDecoder<Void> {
  protected void decode(
      ChannelHandlerContext ctx, final ByteBuf buf, List<Object> out) {
    // A decoder for reading serialized ads from a byte channel.
    // TODO: We should use a smarter method of finding message ends.
    if (actualReadableBytes() == 0)
      return;

    Ad ad = Ad.parse(new InputStream() {
      int i = 0, len = actualReadableBytes();
      public int read() {
        return (i++ == len) ? -1 : buf.readByte();
      }
    });
  }

  public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
    // We got a parse error. This may lead to desynchronization, but we should
    // leave closing of the connection up to another handler.
    t.printStackTrace();
  }
}
