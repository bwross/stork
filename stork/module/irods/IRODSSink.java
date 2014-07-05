package stork.module.irods;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.irods.jargon.core.pub.io.IRODSFile;


import stork.feather.*;
import stork.feather.util.ThreadBell;

public class IRODSSink extends Sink<IRODSResource>{
	ExecutorService excutor = java.util.concurrent.Executors.newSingleThreadExecutor();
	IRODSStreams stream = null;
	Bell startbell = null;
	public IRODSFile destFile = null;
	public IRODSSink(IRODSResource destination) {
		super(destination);
	}

	public Bell start() throws Exception{
		
		return startbell = new ThreadBell(excutor) {
			public Object run() throws Exception{
				String irodsSource = destination().path.toString();
				destFile = destination().session.irodsFileFactory.instanceIRODSFile(irodsSource);
				stream = destination().session.stream;
				System.out.println("sink stream " + stream);
				stream.open2Write(destFile);
				System.out.println("(2) IRODSSink started");
				/*
				String test = "message for test";
				byte[] data = test.getBytes();
				int len = data.length;
				stream.streamBytesToFile(data, len);
				stream.close();
				*/
				return null;
			} public void fail(Throwable t) {
				t.printStackTrace();
			}
		}.startOn(destination().session.initialize());
	}
	
	@Override
	public Bell drain(final Slice slice) {
		return startbell.promise(new Bell(){
			public void done(){
				final byte[] data = slice.asBytes();
				final int length = slice.length();
				try {
					//System.out.println(new String(data, "UTF-8"));
					excutor.execute(new Runnable(){
						@Override
						public void run(){
							try {
								if(null == data) return;
								System.out.println("write to stream");
								stream.streamBytesToFile(data, length);
							} catch (Exception e) {
								e.printStackTrace();
							}							
						}
					});
				} catch (Exception e) {
					e.printStackTrace();
					//return new Bell(e);
				} 
			}
		}).debugOnRing();
		//return null;
	}
	

	@Override
	protected void finish() {
		if (stream != null)
			try {
				//while (!excutor.isTerminated()) {}				
				excutor.execute(new Runnable(){
					public void run(){
						try {
							stream.close();
						} catch (Exception e) {
							e.printStackTrace();
						}	
					}
				});
				excutor.shutdown();
				while (!excutor.isTerminated()) {System.out.println("wait for terminate 1");}
				System.out.println("terminate 1 finished!");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	} protected void finish(Throwable t) {
		if (stream != null)
			try {
				//while (!excutor.isTerminated()) {}				
				excutor.execute(new Runnable(){
					public void run(){
						try {
							stream.close();
						} catch (Exception e) {
							e.printStackTrace();
						}	
					}
				});
				excutor.shutdown();
				while (!excutor.isTerminated()) {System.out.println("wait for terminate 2");}
				System.out.println("terminate 2 finished!");
			} catch (Exception e) {
				e.printStackTrace();
			}
	}	
}
