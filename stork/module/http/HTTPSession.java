package stork.module.http;

import java.util.concurrent.ExecutionException;

import stork.feather.Bell;
import stork.feather.Path;
import stork.feather.Resource;
import stork.feather.Session;
import stork.feather.Stat;
import stork.feather.URI;
import stork.feather.util.HexDumpResource;
import stork.module.ftp.FTPModule;
import stork.module.ftp.FTPResource;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * A HTTP download session
 * 
 * @see {@link Session}
 */
public class HTTPSession extends Session<HTTPSession, HTTPResource> {
	
	protected EventLoopGroup workGroup;
	protected HTTPBuilder builder;    

	/**
	 * A constructor of {@code HTTPSession} with a domain described
	 * by {@link URI}.
	 * 
	 * @param uri A URL with host name
	 */
    public HTTPSession(URI uri) {
    	super(uri);
    	workGroup = new NioEventLoopGroup();
    }
    
	@Override
	public HTTPResource select(Path path) {		
		HTTPResource resource = new HTTPResource(this, path);

		return resource;
	}
	
	@Override
	public Bell<HTTPSession> initialize() {
		return new Bell<Object> () {{
			// Initialize the connection
			builder = new HTTPBuilder(HTTPSession.this);
			builder.onConnectBell.promise(this);
			
			// Set up the session close reaction
			builder.onCloseBell.new Promise() {
				
				@Override
				public void always() {builder.tapBellQueue.clear();}
			};
		}}.as(this);
	}
 
	@Override
    public void cleanup() {
    	try {
			builder.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
    	//URI u = URI.create("http://www.indeed.com");	// Test 'keep-alive' connection
    	Path p1 = Path.create("l-Rochester,-NY-jobs.html");
    	Path p2 = Path.create("l-Buffalo,-NY-jobs.html");
    	//URI u = URI.create("http://www.nytimes.com");	// Test 'close' connection
    	//URI u = URI.create("http://bing.co");	// Test host 'moved' fault
    	URI u = URI.create("http://www.microsoft.com");	// Test path 'moved' fault
    	Path p3 = Path.create("pages/national/index.html");
    	Path p4 = Path.create("pages/nyregion/index.html");
    	Path p5 = Path.create("");
        HTTPSession s = new HTTPSession(u).initialize().get();
        /*
        HTTPResource r = s.select("l-Buffalo,-NY-jobs.html");
        	r.tap().start().get();
	        //r.tap().pause();
	        //Thread.sleep(1000);
	        //r.tap().resume();
        s.select(p1).tap().start();
        s.select(p2).tap().start();
        s.select(p1).tap().start();
        s.select(p2).tap().start();
        s.select(p1).tap().start();*/

        // FTP demo
        //FTPResource dest = new FTPModule().select(
        //	URI.create("ftp://didclab-ws8.cse.buffalo.edu/index.html")
        //);
        //dest.initialize().sync();
        //s.select(p3).tap().attach(dest.sink()).tap().start().sync();
        
        HTTPResource r1 = s.select(p3);
        HTTPResource r2 = s.select(p4);
        Bell<Stat> b1 = r1.stat();
        Bell<Stat> b2 = r2.stat();
        r1.tap().attach(new HexDumpResource().sink()).tap().start().sync();
        System.out.println(b1.get().path() + " size- " + b1.get().size() + " time- " + b1.get().time);
        r2.tap().attach(new HexDumpResource().sink()).tap().start();        
        System.out.println(b2.get().path() + " size- " + b2.get().size() + " time- " + b2.get().time);

        s.cleanup();
        s.select(p5).tap().attach(new HexDumpResource().sink()).tap().start();
        s.select(p5).tap().attach(new HexDumpResource().sink()).tap().start();
        s.select(p5).tap().attach(new HexDumpResource().sink()).tap().start();
        s.select(p5).tap().attach(new HexDumpResource().sink()).tap().start();
        
        Thread.sleep(3000);System.out.println(
		s.builder.channel.isActive() + " " +
		s.builder.channel.isInputShutdown() + " " +
		s.builder.channel.isOutputShutdown() + " " +
		s.builder.channel.isOpen() + " " +
		s.builder.channel.toString());
        s.workGroup.shutdownGracefully();
    }
}