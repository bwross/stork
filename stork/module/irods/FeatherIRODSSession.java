package stork.module.irods;

import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.connection.IRODSSession;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.CollectionAndDataObjectListAndSearchAO;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;

import stork.ad.Ad;
import stork.feather.*;
import stork.feather.util.*;
import stork.module.ftp.FTPModule;

public class FeatherIRODSSession extends Session<FeatherIRODSSession,IRODSResource> {
	
	IRODSStreams stream = null;
	IRODSAccount irodsAccount = null;
	IRODSSession irodsSession = null;
	IRODSFileSystem irodsFileSystem = null;
	IRODSFileFactory irodsFileFactory = null;	
	
	CollectionAndDataObjectListAndSearchAO actualCollection;
	public FeatherIRODSSession(URI uri, Credential credential) {
		super(uri, credential);
	}

	protected Bell<FeatherIRODSSession> initialize() {
		final String host = uri.host();
		final int port = uri.port();
		final String userName = uri.user();
		final String password = uri.password();
		final String homeDirectory = "/UniversityAtBuffaloZone/home/didclab/";
		final String userZone = "UniversityAtBuffaloZone";
		final String defaultStorageResource = "didclab-ws2Resc";
		
		return new ThreadBell<FeatherIRODSSession>(){
			public FeatherIRODSSession run() throws Exception{
				
				irodsAccount = new IRODSAccount(host, port, userName, password, homeDirectory,
						userZone, defaultStorageResource);
				irodsFileSystem = IRODSFileSystem.instance();
				irodsSession = irodsFileSystem.getIrodsSession();
				irodsFileFactory = irodsFileSystem.getIRODSFileFactory(irodsAccount);
				
				stream = new IRODSStreams(irodsSession, irodsAccount);
				actualCollection = irodsFileSystem.getIRODSAccessObjectFactory()
						.getCollectionAndDataObjectListAndSearchAO(irodsAccount);
				System.out.println("host: " +host + "port: "+ port + "username: " +userName + "password: " +password + 
						"homeDirectory: " +homeDirectory + "userZone: " + userZone + "defaultStorageResource: " + defaultStorageResource);
				/*
				IRODSFile sourceFile = irodsFileFactory.instanceIRODSFile("/UniversityAtBuffaloZone/home/didclab/checkresult");
				stream.open2Read(sourceFile);
				byte[] buf = stream.streamFileToBytes();
        		Slice slice = new Slice(buf);
        		System.out.println(new String(buf, "UTF-8"));*/
				
				/*
				IRODSFile destFile = irodsFileFactory.instanceIRODSFile("/UniversityAtBuffaloZone/home/public/checkresult.copy");				
				stream.open2Write(destFile);
				String test = "message for test";
				byte[] data = test.getBytes();
				int len = data.length;
				stream.streamBytesToFile(data, len);
				stream.close();*/
				System.out.println("(1) finished initi, stream:" + stream);
				return FeatherIRODSSession.this;
			} public void done() {
				System.out.println("Init complete!");
			}
		}.start().debugOnRing();
	}

	@Override
	public IRODSResource select(Path path) {
		return new IRODSResource(this, path);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		URI urisource = URI.create("irods://didclab:didclab@didclab-ws2.cse.buffalo.edu:1247//UniversityAtBuffaloZone/home/didclab/checkresult");
		IRODSResource source = new IRODSModule().select(urisource);
		//Resource source = new FTPModule().select(URI.create("ftp://didclab-ws8/small.txt"));
		URI uridest = URI.create("irods://didclab:didclab@didclab-ws2.cse.buffalo.edu:1247//UniversityAtBuffaloZone/home/public/small.txt");
		//IRODSResource dest = new IRODSModule().select(uridest);
		Resource dest = new FTPModule().select(URI.create("ftp://didclab-ws8/checkresult"));
		//Transfer transfer = source.transferTo(new HexDumpResource());
		Transfer transfer = source.transferTo(dest);
		//LocalSession local = new LocalSession(Path.create("/home/bing/checkresult"));
		//Transfer transfer = local.root().transferTo(dest);
		//
		//transfer.starter.ring();
		//transfer.onStop().sync();
		source.list().new ForEach() {
			public void each(String name) {
				System.out.println("Got: "+name);
			}
		};
		
	}

}
