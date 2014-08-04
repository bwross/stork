package stork.feather.util;

import java.util.*;
import java.nio.*;
import java.nio.charset.*;

import io.netty.buffer.*;
import io.netty.util.*;

import stork.feather.*;

/**
 * A utility class for creating {@link Pipe}s, {@link Tap}s, and {@link Sink}s
 * for various purposes.
 */
public final class Pipes {
  private Pipes() { }

  /**
   * Create an anonymous {@code Tap} which emits a single {@code Slice} to an
   * attached {@code Sink}. If {@code slice} is {@code null}, the returned
   * {@code Tap} will initialize the attached {@code Sink}, but will not emit
   * any {@code Slice}s.
   *
   * @param slice the {@code Slice} emitted by the returned {@code Tap}.
   * @return An anonymous {@code Tap} which will emit {@code slice}.
   */
  public static Tap tapFromSlice(final Slice slice) {
    return Resources.fromSlice(slice).tap();
  }

  /**
   * Create a {@code Tap} which emits the given {@code Slice} for the given
   * {@code Resource} {@code root}.
   */
  public static <R extends Resource<?,R>>
  Tap<R> tapFromSlice(R root, Slice slice) {
    final Slice s = slice;
    return new Tap<R>(root) {
      public Bell start(Bell bell) {
        return bell.new Promise() {
          public void done() {
            if (s != null) drain(s);
            finish();
          }
        };
      } public void pause(Bell bell) {
        // Just no-op for now...
      }
    };
  }

  /**
   * Create an anonymous {@code Tap} which emits the {@code String}
   * representation of an object encoded as UTF-8.
   *
   * @param object the object to stringify and emit.
   * @return An anonymous {@code Tap} which will emit {@code object} as a
   * {@code String} encoded using UTF-8.
   */
  public static Tap tapFromString(Object object) {
    return tapFromString(object, CharsetUtil.UTF_8);
  }

  /**
   * Create an anonymous {@code Tap} which emits the {@code String}
   * representation of an object encoded using {@code charset}.
   *
   * @param object the object to stringify and emit.
   * @param charset the {@link Charset} to use for encoding.
   * @return An anonymous {@code Tap} which will emit {@code object} as a
   * {@code String} using {@code charset}.
   */
  public static Tap tapFromString(Object object, Charset charset) {
    if (object == null)
      return Resources.fromSlice(null).tap();
    CharBuffer cb = CharBuffer.wrap(object.toString());
    ByteBufAllocator allo = UnpooledByteBufAllocator.DEFAULT;
    ByteBuf bb = ByteBufUtil.encodeString(allo, cb, charset);
    return Resources.fromSlice(new Slice(bb)).tap();
  }

  /**
   * A {@code Pipe} which aggregates multiple, randomly-ordered {@code Slices}
   * into a single {@code Slice} which it drains on completion.
   */
  public static Pipe aggregatorPipe() {
    return new Pipe() {
      private List<ByteBuf> list = new LinkedList<ByteBuf>();

      public Bell drain(Slice slice) {
        list.add(slice.asByteBuf());
        return null;
      }

      public void finish() {
        ByteBuf[] array = list.toArray(new ByteBuf[0]);
        ByteBuf buf = Unpooled.wrappedBuffer(array);
        try {
          super.drain(new Slice(buf));
        } catch (Exception e) {
          // What to do here...
        }
      }
    };
  }

  /**
   * A {@code Sink} which receives and parses an {@code Ad}.
   */
  public static class AggregatorSink extends Sink {
    private Bell<Slice> bell = new Bell<Slice>();
    private List<ByteBuf> list = new LinkedList<ByteBuf>();

    public AggregatorSink(Resource r) { super(r); }

    public Bell<Slice> bell() {
      return bell;
    }

    public Bell drain(Slice slice) {
      list.add(slice.asByteBuf());
      return null;
    }

    public void finish() {
      ByteBuf[] array = list.toArray(new ByteBuf[0]);
      ByteBuf buf = Unpooled.wrappedBuffer(array);
      bell.ring(new Slice(buf));
    }
  }

  /**
   * Get an {@code AggregatorSink} for an anonymous {@code Resource}.
   */
  public static AggregatorSink aggregatorSink() {
    return new AggregatorSink(Resources.anonymous());
  }
}
