package stork.module.http;

import stork.feather.URI;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Handles connection test channel.
 */
class HTTPTestHandler extends ChannelHandlerAdapter {
	
	private HTTPUtility utility;
	private int code;	// Action code
	
	public HTTPTestHandler(HTTPUtility utility) {
		this.utility = utility;
	}
	
	@Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
		if (msg instanceof HttpResponse) {
			HttpResponse response = (HttpResponse) msg;
			HttpResponseStatus status = response.getStatus();
			try {
				if (HTTPCodes.isMoved(status)) {
					URI uri = URI.create(
							response.headers().get(HttpHeaders.Names.LOCATION));
					utility.setUri(URI.create(uri.endpoint()));
					code = 1;
					throw new HTTPException(utility.getHost() + " " + status.toString());
				} else if (HTTPCodes.isNotFound(status)) {
					code = 2;
					throw new HTTPException(utility.getHost() + " " + status.toString());
				} else if (HTTPCodes.isInvalid(status)) {
					code = 3;
					throw new HTTPException(
							utility.getHost() + " HEADER method unsupported");
				} else if (HTTPCodes.isOK(status)) {
					// Valid HTTP server found
					code = 0;
					if (response.headers()
							.get(HttpHeaders.Names.CONNECTION)
							.equalsIgnoreCase(HttpHeaders.Values.CLOSE.toString())) {
						utility.setKeepAlive(false);
					} else {
						utility.setKeepAlive(true);
					}
				}
			} catch (HTTPException e) {
				System.err.println(e.getMessage());
			}
		}
		if (msg instanceof HttpContent) {
			endTest(ctx);
		}
	}
	
	private void endTest(ChannelHandlerContext ctx) {
		if (utility.isKeepAlive && (code == 0)) {
			utility.onEstablishBell.ring();
		} else {
			final HTTPChannel ch = (HTTPChannel) ctx.channel();
			ch.disconnect();
			ch.close().addListener(new GenericFutureListener<ChannelFuture> () {

				@Override
				public void operationComplete(ChannelFuture future) {
					
					switch(code) {
					case 3: 
						if (utility.testMethod.equals(HttpMethod.HEAD)) {
							utility.testMethod = HttpMethod.GET;
						} else {
							utility.close();
							System.err.println("Error: Bad request on " + 
									utility.getHost() +
									". The session has been closed.");
							break;
						}
					case 1: utility.setupWithTest();
					case 2: break;
					case 0: 
						utility.onInactiveBell.ring();
						utility.setupWithoutTest();
						break;
					default:
						utility.close();
						System.err.println("Error: Unimplemented response code on "
								+ utility.getHost() + ". The session has been closed.");
						break;
					}
				}
			});
		}
	}
}
