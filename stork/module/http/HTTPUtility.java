package stork.module.http;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.GenericFutureListener;
import stork.feather.Bell;
import stork.feather.Path;
import stork.feather.URI;
import stork.module.http.HTTPResource.HTTPTap;

/**
 * HTTP channel connected to a given host
 */
class HTTPUtility {
	// Bell used to indicate the close state of this channel
    protected final Bell<Void> onCloseBell = new Bell<Void> ();
    // Bell rung when connection property is set
    protected Bell<Void> onEstablishBell;
    // Bell rung when the channel finally connects.
    protected Bell<Void> onConnectBell;
    // Bell rung when the channel goes to inactive state
    protected Bell<Void> onInactiveBell;
    protected boolean isKeepAlive;
    protected HttpMethod testMethod;
    protected Queue<Bell<Void>> tapBellQueue;
    
    private HTTPChannel channel;
    private Bootstrap boot;
    private URI uri;
    private int port;
    
    // Constructor for base HTTP channel
    public HTTPUtility(HTTPSession session) {        
        try {
            boot = new Bootstrap();
            boot.group(session.workGroup)
             .channel(HTTPChannel.class)
             .handler(new HTTPInitializer(session.uri.scheme(), this));
            
            // Channel setup
            testMethod = HttpMethod.HEAD;
            isKeepAlive = true;
        	onEstablishBell = new Bell<Void>() {
        		@Override
        		protected void done() {//System.out.println("test done"); // TODO test
        			updatePipeline();
        			channel.config().setKeepAlive(isKeepAlive);
        		}
        	};
    		onConnectBell = new Bell<Void>();
    		onInactiveBell = new Bell<Void>();
            setUri(session.uri);
            setupWithTest();
            
            // Tap bells queue setup
            tapBellQueue = new ConcurrentLinkedQueue<Bell<Void>>();
        } catch (HTTPException e) {
        	System.err.println(e.getMessage());
        }
    }
    
    public Bell<?> onClose() {
    	return onCloseBell;
    }
    
    public boolean isClosed() {
    	return onCloseBell.isDone();
    }
    
    /* Close and clear up the channel */
    public void close() {
    	if (!isClosed()) {
	    	channel.clear();
	    	onCloseBell.ring();
    	}
    }
    
    public boolean isKeepAlive() {
    	return isKeepAlive;
    }
    
    protected void setKeepAlive(boolean v) {
        	isKeepAlive = v;
    }
    
    /* Establish a new socket connection with connection test*/
    protected void setupWithTest() {
		ChannelFuture future = boot.connect(uri.host(), port);
		future.addListener(new GenericFutureListener<ChannelFuture>() {

			@Override
			public void operationComplete(ChannelFuture f) {
				if (f.isSuccess()) {
					channel = (HTTPChannel) f.channel();
					testConnection();
					onEstablishBell.promise(onConnectBell);
				} else {
					onConnectBell.ring(f.cause());
				}
			}
		});
    }
    
    /* Establish a new socket connection without testing*/
    protected void setupWithoutTest() {
		ChannelFuture future = boot.connect(uri.host(), port);
		future.addListener(new GenericFutureListener<ChannelFuture>() {
			@Override
			public void operationComplete(ChannelFuture f) {
				if (f.isSuccess()) {
					channel = (HTTPChannel) f.channel();
					onEstablishBell.ring();
				} else {
					onConnectBell.ring(f.cause());
				}
			}
		});
    }
    
    /* 
     * Create a new socket connection in case that keep-alive option 
     * is not available.
     * */
    protected Bell<HTTPChannel> resetConnection(HTTPTap tap) {
    	// Bell rung when the channel has been established
    	final HTTPTap localTap = tap;
		final Bell<HTTPChannel> connectBell = new Bell<HTTPChannel>() {
			
			@Override
			protected void done() {
				channel.addChannelTask(localTap);
				channel.writeAndFlush(prepareGet(localTap.getPath()));
			}
		};

    	// Start a new connection immediately if the channel is in idle status
    	synchronized (tapBellQueue) {
    		if (onInactiveBell.isDone() && tapBellQueue.isEmpty()) {
    			onInactiveBell = new Bell<Void>();
    			ChannelFuture f = boot.connect(uri.host(), port);
    			f.addListener(new GenericFutureListener<ChannelFuture>() {

    				@Override
    				public void operationComplete(ChannelFuture f) {
    					if (f.isSuccess()) {
    						channel = (HTTPChannel) f.channel();
    						updatePipeline();
    						connectBell.ring(channel);
    					} else {
    						connectBell.ring(f.cause());
    					}
    				}
    			});
    			
    			return connectBell;
    		}
    	}

    	// Otherwise, add the resource request to the waiting queue
    	// Bell rung when the channel finishes previous task
    	Bell<Void> waitBell	= new Bell<Void>();
    	Bell<Void> createBell = new Bell<Void>() {
    		
    		@Override
    		protected void done() {
    			ChannelFuture f = boot.connect(uri.host(), port);
    			f.addListener(new GenericFutureListener<ChannelFuture>() {

    				@Override
    				public void operationComplete(ChannelFuture f) {
    					if (f.isSuccess()) {
    						channel = (HTTPChannel) f.channel();
    						updatePipeline();
    						connectBell.ring(channel);
    					} else {
    						connectBell.ring(f.cause());
    					}
    			    	onInactiveBell = new Bell<Void>();
    				}
    			});
    		}
    	};
    	
    	waitBell.promise(createBell);
    	tapBellQueue.offer(waitBell);
    	return connectBell;
    }
    
    protected void setUri(URI uri) {
    	String strUri = uri.toString();
    	if (strUri.endsWith("/")) {
    		strUri = strUri.substring(0, strUri.length() - 1);
    	}
    	this.uri = URI.create(strUri);
    	try {
			port = analURI(uri);
		} catch (HTTPException e) {
			System.err.println(e.getMessage());
		}
    }
    
    protected String getHost() {
    	return uri.host();
    }
    
    protected int getPort() {
    	return port;
    }
    
    protected HTTPChannel getChannel() {
    	return channel;
    }
    
    /* Prepare request message to be sent*/
    protected HttpRequest prepareGet(Path path) {
		HttpRequest request = new DefaultFullHttpRequest( 
	    		HttpVersion.HTTP_1_1, HttpMethod.GET, "" + path);
		request.headers().set(HttpHeaders.Names.HOST, this.uri.host());
		request.headers().set(HttpHeaders.Names.USER_AGENT, "Stork");
		
		return request;
    }
    
    /* Remove test handler, and install message receiver handler */
    protected void updatePipeline() {
    	channel.pipeline().remove("Tester");
		channel.pipeline().addFirst("Timer", new ReadTimeoutHandler(10));
		channel.pipeline().addLast(
				"Handler", new HTTPMessageHandler(HTTPUtility.this));
    }
    
    /* Test whether the connection could be keep-alive */
    private void testConnection() {
    	HttpRequest request = prepareGet(uri.path());
    	request.setMethod(testMethod);
    	channel.config().setKeepAlive(true);
    	channel.writeAndFlush(request);
    }
    
    /*
     * Return an appropriate port number from given URL
     */
    private int analURI(URI uri) throws HTTPException {
    	int port = -1;
    	
		if (uri.host() == null) {
			throw new HTTPException("Error: null host name");
		}
		
		if (uri.port() == -1) {
			if (uri.scheme().equalsIgnoreCase("http")) {
        		if (uri.port() == -1) {
        			port = 80;
        		}
			} else if (uri.scheme().equalsIgnoreCase("https")) {
				port = 443;
			}
		} else {
			port = uri.port();
		}
		
		if (port == -1) {
			throw new HTTPException("Error: incorrect port number");
		}
		
		return port;
    }
}
