package stork.module.http;

import java.text.ParseException;
import java.util.Date;

import stork.feather.Bell;
import stork.feather.Slice;
import stork.feather.Stat;
import stork.feather.URI;
import stork.module.http.HTTPResource.HTTPTap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Message handler receiving status.
 */
enum Status {
	Header,
	Content,
	NotFound,
	Closed
}

/**
 * Handles client-side downstream.
 */
public class HTTPMessageHandler extends ChannelHandlerAdapter {
	
	private HTTPBuilder builder;
	private HTTPTap tap;
	private Status status = Status.Header;
	
	/**
	 * Constructs a data receiver channel
	 * 
	 * @param util {@link HTTPBuilder} that initiates this channel
	 */
	public HTTPMessageHandler(HTTPBuilder  util) {
		this.builder = util;
	}
	
	/**
	 * Receives meta data and data from remote HTTP server.
	 * 
	 * @param ctx handler context of this channel
	 * @param msg received content
	 */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
		final HTTPChannel ch = (HTTPChannel) ctx.channel();

    	if (msg instanceof HttpResponse) {
    		HttpResponse resp = (HttpResponse) msg;
    		String connection = resp.headers().get(HttpHeaders.Names.CONNECTION);
    		
	    	if (status == Status.Header) {
	    		// This is the first packet of response, needs to know the 
	    		// requesting resource of it.
	    		status = Status.Content;
	    		tap = ch.tapQueue.poll();
	    	}

			// Directly closes local end if remote
			// server does not comprehend the sent close request
			if (status == Status.Closed) {
		    	if (connection == null) {
		    		ch.close();
		    		return;
		    	} else if (connection.equals(HttpHeaders.Values.KEEP_ALIVE)) {
		    		ch.close();
		    		return;
		    	}
			}

			// Normally, this shouldn't happen. It is assumed that
			// a HTTP server always remains in the same connection state.
			if (connection != null) {
				if (connection.equals(HttpHeaders.Values.CLOSE) &&
						builder.isKeepAlive()) {
					for (HTTPTap tap: ch.tapQueue) {
						builder.tryResetConnection(tap);
					}
					builder.setKeepAlive(false);
				} else if (connection.equals(HttpHeaders.Values.KEEP_ALIVE)) {
					//	!builder.isKeepAlive()) {
					//ch.addChannelTask(builder.tapBellQueue.poll());
				}
			}
			
			caseHandler(resp, ch);
    		
			if (status == Status.Content) {
	    		if (!tap.hasStat()) {
	    			// The resource this tap belongs to has not 
	    			// received meta data yet. Do it now.
	    			tap.setStat(getStat(resp));
	    		}
			}
    		
    		ch.setReadable(false);
    		tap.sinkReadyBell.new Promise() {
    			public void done() { ch.setReadable(true); }
    		};
    	}
    	else if (msg instanceof HttpContent) {
	    	HttpContent content = (HttpContent) msg;

    		if (status == Status.Content) {
	    		ByteBuf buf = content.content();
	    		Slice slice = new Slice(buf);
	    		
    			// Ring the bell once the received data is ready
    			tap.onStartBell.ring();
	    		
	    		tap.drain(slice);
    		}

    		if (content instanceof LastHttpContent) {
    			if (status == Status.Content) {
    				tap.finish();
    			}
	    		if (builder.isKeepAlive()) {
	    			if (builder.onCloseBell.isDone()) {
	    				// Creates a close request to
	    				// remote server if session is closed
	    				if (status != Status.Closed) {
	    					notifyClose(ch);
	    				}
	    			} else {
	    				status = Status.Header;	// Reset status.
	    			}
	    		}
    		}
    	}
    }
    
    /**
     * Called whenever this current channel is in inactive state.
     * It usually happens after finishing the data receiving from server.
     * 
     * @param ctx handler context of this channel
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
    	final HTTPChannel ch = (HTTPChannel) ctx.channel();

    	ch.close().addListener(new GenericFutureListener<ChannelFuture>() {

    		@Override
			public void operationComplete(ChannelFuture arg0) throws Exception {
				if (!builder.isKeepAlive()) {
					if (builder.onCloseBell.isDone()) {
						ch.clear();
					} else {
						synchronized(ch) {
							Bell<Void> bell = builder.tapBellQueue.poll();
							if (bell == null) {
								ch.onInactiveBell.ring();
							} else {
								ch.onInactiveBell.promise(bell);
								ch.onInactiveBell.ring();
							}
						}
					}
				} else {
					// close this channel resource
					ch.onInactiveBell.ring();
					ch.clear();
					ch.close();
				}
			}
		});
    }

    /**
     * Called when an exception is caught, usually by a read time-out from
     * remote server.
     * 
     * @param ctx handler context of this channel
     * @param cause {@link Throwable} that tells the exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
    	tap.finish();
    	if (cause instanceof ReadTimeoutException) {
    		ctx.fireChannelInactive();
    	} else {
    		cause.printStackTrace();
    		ctx.close();
    	}
    }
    
    // Handles various abnormal response codes.
    private void caseHandler(HttpResponse response, HTTPChannel channel) {
    	HttpResponseStatus status = response.getStatus();
		try {
			if (HTTPResponseCode.isMoved(status)) {
				this.status = Status.NotFound;
				// Redirect to new location
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
				if (builder.isKeepAlive()) {
					channel.addChannelTask(tap);
					channel.writeAndFlush(builder.prepareGet(tap.getPath()));
				} else {
					builder.tryResetConnection(tap);
				}
				throw new HTTPException(
						tap.getPath() + " " + status.toString());
			} else if (HTTPResponseCode.isNotFound(status)) {
				throw new HTTPException(
						tap.source().uri() + " " + status.toString());
			}
		} catch (HTTPException e) {
			System.err.println(e.getMessage());
		}
    }
    
    // Gathers meta data information
    private Stat getStat(HttpResponse response) {
    	Stat stat = new Stat(tap.getPath().toString());
		String length = response.headers().
				get(HttpHeaders.Names.CONTENT_LENGTH);
		Date time = null;
		
		try {
			time = HttpHeaders.getDate(response);
		} catch (ParseException e) {
			// This means date meta data is not available
		}
		stat.dir = false;
		stat.file = true;
		stat.link = tap.getPath();
		stat.size = (length == null) ? -1l : Long.valueOf(length);
		stat.time = (time == null) ? -1l : time.getTime();
		
		return stat;
    }
    
    // Notifies remote HTTP server to close connection
    private void notifyClose(HTTPChannel ch) {
		ch.clear();
		HttpRequest request =
				builder.prepareGet(tap.getPath());
		HttpHeaders.setKeepAlive(request, false);
		ch.writeAndFlush(request);
		status = Status.Closed;
    }
}