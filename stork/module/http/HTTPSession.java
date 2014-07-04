package stork.module.http;

import java.util.concurrent.ExecutionException;

import stork.feather.Bell;
import stork.feather.Path;
import stork.feather.Resource;
import stork.feather.Session;
import stork.feather.URI;
import stork.feather.util.HexDumpResource;
import stork.module.ftp.FTPModule;
import stork.module.ftp.FTPResource;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

/**
 * HTTP download session
 */
public class HTTPSession extends Session<HTTPSession, HTTPResource> {
	
	protected EventLoopGroup workGroup;
	protected HTTPUtility utility;    

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
			utility = new HTTPUtility(HTTPSession.this);
			utility.onConnectBell.promise(this);
			
			// Set up the channel close reaction
			utility.onClose().new Promise() {
				
				@Override
				public void always() {workGroup.shutdownGracefully();}
			};
		}}.as(this);
	}
 
    public void finalize() {
    	// TODO handle the closed channel resource
    	utility.close();
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
    	//URI u = URI.create("http://www.indeed.com");	// Test 'keep-alive' connection
    	Path p1 = Path.create("l-Rochester,-NY-jobs.html");
    	Path p2 = Path.create("l-Buffalo,-NY-jobs.html");
    	//URI u = URI.create("http://www.nytimes.com");	// Test 'close' connection
    	URI u = URI.create("http://bing.co");	// Test host 'moved' fault
    	//URI u = URI.create("http://www.microsoft.com");	// Test path 'moved' fault
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
        
        s.select(p5).tap().attach(new HexDumpResource().sink()).tap().start().sync();
        s.select(p5).tap().attach(new HexDumpResource().sink()).tap().start().sync();
        s.select(p5).tap().attach(new HexDumpResource().sink()).tap().start().sync();
        s.select(p5).tap().attach(new HexDumpResource().sink()).tap().start().sync();
        
        //Thread.sleep(6000);
        //s.finalize();
    }
}