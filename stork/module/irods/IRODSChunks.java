package stork.module.irods;

public class IRODSChunks {
	public byte[] bytes = null;
	public int length = -1;
	public IRODSChunks(final byte[] bytes, final int length){
		this.bytes = bytes;
		this.length = length;
	}
}
