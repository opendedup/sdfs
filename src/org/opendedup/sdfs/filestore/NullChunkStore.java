package org.opendedup.sdfs.filestore;

import java.io.IOException;

/**
 * 
 * @author Sam Silverberg
 * 
 *         The NullChunkStore does not write data do a filesystem at all. It can
 *         be used for testing.
 * 
 * 
 */

public class NullChunkStore implements AbstractChunkStore {

	public long bytesRead() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long bytesWritten() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void closeStore() {
		// TODO Auto-generated method stub

	}

	@Override
	public void expandFile(long length) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		// TODO Auto-generated method stub
		return new byte[0];
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "null";
	}

	@Override
	public long reserveWritePosition(int len) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setName(String name) {
		// TODO Auto-generated method stub

	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void writeChunk(byte[] hash, byte[] chunk, int len, long start)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addChunkStoreListener(AbstractChunkStoreListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void claimChunk(byte[] hash, long start) throws IOException {
		// TODO Auto-generated method stub

	}

}
