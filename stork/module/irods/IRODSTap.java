package stork.module.irods;
import org.irods.jargon.core.pub.io.IRODSFile;


import stork.feather.*;
import stork.feather.util.BellLoop;
import stork.feather.util.ThreadBell;

public class IRODSTap extends Tap <IRODSResource>{
	public IRODSTap(IRODSResource root){
		super(root);
	}
	
	@Override
	protected Bell start(final Bell start){
		return new ThreadBell() {
			public Object run() throws Exception {
				System.out.println("iRODSTap.start.run start");
				String irodsSource = source().path.toString();
				IRODSFile sourceFile = source().session.irodsFileFactory.instanceIRODSFile(irodsSource);
				final IRODSStreams stream = source().session.stream;
				System.out.println("tap stream " + stream);
				stream.open2Read(sourceFile);
				System.out.println("iRODSTap.start.run finish");
				start.sync();
				
				while(true){
					byte[] buf = stream.streamFileToBytes();
	        		if(null == buf) {
	        			break;
	        		} else {
		        		Slice slice = new Slice(buf);
		        		//System.out.println(new String(buf, "UTF-8"));
						drain(slice).debugOnRing().sync();
	        		}
				}
				finish();
				/*
				new BellLoop() {
					Bell lock = Bell.rungBell();
					boolean notdone = true;
					public Bell lock() { return lock.debugOnRing(); }
					public boolean condition() { return notdone; }
					public void body() throws Exception {
						byte[] buf = stream.streamFileToBytes();
		        		if(null == buf) {
		        			finish();
		        			notdone = false;
		        		} else {
			        		Slice slice = new Slice(buf);
			        		System.out.println(new String(buf, "UTF-8"));
							lock = drain(slice);
		        		}
					} public void fail(Throwable t) {
						finish(t);
					}
				}.start(start.debugOnRing());*/
				return null;
			}
			public void fail(Throwable t){
				t.printStackTrace();
			}
		}.startOn(source().initialize());
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}


}
