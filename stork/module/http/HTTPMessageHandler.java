package stork.module.http;

import java.util.concurrent.atomic.AtomicInteger;

import stork.feather.Bell;
import stork.feather.Slice;
import stork.feather.URI;
import stork.module.http.HTTPResource.HTTPTap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Handles a client-side channel.
 */
class HTTPMessageHandler extends ChannelHandlerAdapter {
	
	private HTTPUtility utility;
	private HTTPTap tap;
	private AtomicInteger messageStatus;	// '0' for the first response packet
											// '1' for the subsequent responses
	
	public HTTPMessageHandler(HTTPUtility  util) {
		this.utility = util;
		messageStatus = new AtomicInteger(0);
	}
	
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

		final HTTPChannel ch = (HTTPChannel) ctx.channel();

    	if (messageStatus.compareAndSet(0, 1)) {
    		// This is the first packet of a response, need to know the 
    		// resource request of it.
    		tap = ch.tapQueue.poll();
    	}

    	if (msg instanceof HttpResponse) {
    		HttpResponse resp = (HttpResponse) msg;
    		String connection = resp.headers().get(HttpHeaders.Names.CONNECTION);

			if (connection != null && connection.equals(HttpHeaders.Values.CLOSE)) {
				if (utility.isKeepAlive()) {
					// Normally, this shouldn't happen. It is assumed that
					// a http server always remain in the connection state
					// what it was.
    				synchronized (ch) {
						for (HTTPTap tap: ch.tapQueue) {
							utility.resetConnection(tap);
						}
						utility.setKeepAlive(false);
    				}
				}
			}
			
			caseHandler(resp, ch);
			/*
    		if (HttpHeaders.isTransferEncodingChunked(resp)) {
    			System.out.println("CHUNKED CONTENT {");
    		} else {
    			System.out.println("CONTENT {");
    		}*/ // TODO print header
    		
    		ch.setReadable(false);
    		tap.sinkReadyBell.new Promise() {
    			public void done() { ch.setReadable(true); }
    		};
    	}
		
    	if (msg instanceof HttpContent) {
	    	HttpContent content = (HttpContent) msg;
	    	
    		if (messageStatus.get() == 1) {
	    		ByteBuf buf = content.content();
	    		Slice slice = new Slice(buf);
	    		
    			// Ring the bell once the received data is ready
    			tap.onStartBell.ring();
	    		
	    		tap.drain(slice);
    		}

    		if (content instanceof LastHttpContent) {
    			//System.out.println(" END OF CONTENT");	// TODO content ends
    			if (messageStatus.get() == 1) {
    				tap.finish();
    			}
	    		if (utility.isKeepAlive()) {
	    			messageStatus.set(0);	// Reset status.
	    		}
    		}
    	}
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
    	final HTTPChannel hc = (HTTPChannel) ctx.channel();
    	if (hc.isOpen()) {
				hc.disconnect();
    	}
    	hc.close().addListener(new GenericFutureListener<ChannelFuture>() {

			@Override
			public void operationComplete(ChannelFuture arg0) throws Exception {
				if (!utility.isKeepAlive()) {
					synchronized (utility.tapBellQueue) {
						Bell<Void> bell = utility.tapBellQueue.poll();
						if (bell == null) {
							utility.onInactiveBell.ring();
						} else {
							utility.onInactiveBell.promise(bell);
							utility.onInactiveBell.ring();
							utility.onInactiveBell = new Bell<Void>();
						}
					}
				} else {
					// close this channel resource
					utility.onInactiveBell.ring();
					utility.close();
				}
			}
		});
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
    	if (cause instanceof ReadTimeoutException) {
    		ctx.fireChannelInactive();
    	} else {
    		cause.printStackTrace();
    		ctx.close();
    	}
    }
    
    // Handles various abnormal reponse codes
    private void caseHandler(HttpResponse response, HTTPChannel channel) {
    	HttpResponseStatus status = response.getStatus();
		try {
			if (HTTPCodes.isMoved(status)) {
				messageStatus.addAndGet(1);
				String newLocation =
						response.headers().get(HttpHeaders.Names.LOCATION);
				newLocation = newLocation.startsWith("/") ?
						newLocation.substring(1) : newLocation;
				URI uri;
				if (!newLocation.contains(":")) {
					uri = URI.create(tap.source().uri() + "/" + newLocation);
				} else {
					uri = URI.create(newLocation);
				}
				tap.setPath(uri.path());
				if (utility.isKeepAlive()) {
					channel.addChannelTask(tap);
					channel.writeAndFlush(utility.prepareGet(tap.getPath()));
				} else {
					utility.resetConnection(tap);
				}
				throw new HTTPException(
						tap.getPath() + " " + status.toString());
			} else if (HTTPCodes.isNotFound(status)) {
				throw new HTTPException(
						tap.source().uri() + " " + status.toString());
			}
		} catch (HTTPException e) {
			System.err.println(e.getMessage());
		}
    }
}