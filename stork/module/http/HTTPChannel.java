package stork.module.http;

import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import stork.module.http.HTTPResource.HTTPTap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.nio.NioSocketChannel;

public class HTTPChannel extends NioSocketChannel {

	// Queue of resource taps	
    protected Queue<HTTPTap> tapQueue = new ConcurrentLinkedQueue<HTTPTap>();

	private boolean readable = true;

	/* Constructors */
	public HTTPChannel(Channel parent, EventLoop eventLoop, SocketChannel socket) {
		super(parent, eventLoop, socket);
	}
	public HTTPChannel(EventLoop eventLoop, SocketChannel socket) {
		super(eventLoop, socket);
	}
	public HTTPChannel(EventLoop eventLoop) {
		super(eventLoop);
	}
	
	/* Add a new task to the queue before starting receiving data */
	protected void addChannelTask(HTTPTap tap) {
		tapQueue.offer(tap);
	}
	
	protected synchronized void setReadable(boolean readable) {
		this.readable = readable;
	}
	
	protected void clear() {
		tapQueue.clear();
		for (HTTPTap tap : tapQueue) {
			tap.onStartBell.cancel();
		}
		tapQueue.clear();
	}
	
	@Override
	protected int doReadBytes(ByteBuf buf) throws Exception {
		if (readable) {
			return buf.writeBytes(javaChannel(), buf.writableBytes());
		} else {
			return 0;
		}
	}
}
