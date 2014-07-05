package stork.module.irods;

import java.util.*;
import java.io.*;

import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.connection.IRODSSession;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.pub.DataObjectAO;
import org.irods.jargon.core.pub.IRODSGenericAO;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.utils.MiscIRODSUtils;



public class IRODSStreams extends IRODSGenericAO{
	private static final int BUFFFERSIZE = 32 * 1024;
	private final byte[] buf;
	final IRODSSession irodsSession;
	final IRODSAccount irodsAccount;
	
	private int bufreadlen = -1;
	private InputStream in = null;
	private OutputStream out = null;
	private DataObjectAO dataObjectAO = null;
	
	public IRODSStreams(final IRODSSession irodsSession,
			final IRODSAccount irodsAccount) throws Exception{
		super(irodsSession, irodsAccount);
		this.irodsSession = irodsSession;
		this.irodsAccount = irodsAccount;
		buf = new byte[BUFFFERSIZE];
	}
	
	public void open2Read(final IRODSFile irodsFile) throws Exception{
		if (irodsFile == null) {
			throw new IllegalArgumentException("null irodsTargetFile");
		}
		if (irodsFile.exists() && irodsFile.isFile()) {
		} else {
			throw new Exception(irodsFile.getName()+" does not exist or is not a file");
		}
		in = getIRODSFileFactory().instanceIRODSFileInputStream(irodsFile);
	}
	
	public void open2Write(final IRODSFile irodsFile) throws Exception{
		if (irodsFile == null) {
			throw new JargonException("targetIrodsFile is null");
		}

		if (irodsFile.getResource().isEmpty()) {
			irodsFile.setResource(MiscIRODSUtils
					.getDefaultIRODSResourceFromAccountIfFileInZone(
							irodsFile.getAbsolutePath(),
							getIRODSAccount()));
		}
		if (dataObjectAO == null) {
			dataObjectAO = getIRODSAccessObjectFactory().getDataObjectAO(
					getIRODSAccount());
		}
		out = getIRODSFileFactory().instanceIRODSFileOutputStream(irodsFile);
	}
	
	public byte[] streamFileToBytes() throws Exception{
		try{
			bufreadlen = in.read(buf);
		}catch (Exception ex){
			ex.printStackTrace();
		}
		if(-1 == bufreadlen) return null;
		return Arrays.copyOf(buf, bufreadlen);
	}
	public void streamBytesToFile(final byte[] bytesToStream, final int length) throws Exception {
		out.write(bytesToStream, 0, length);
		out.flush();		
	}
	
	public void close() throws Exception{
		if(null != dataObjectAO) dataObjectAO = null;
		if(null != in) in.close();
		if(null != out) out.close();
		in = null; out = null;
	}
	
}
