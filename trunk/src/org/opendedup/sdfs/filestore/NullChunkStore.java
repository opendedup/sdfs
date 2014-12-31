package org.opendedup.sdfs.filestore;

import java.io.IOException;


import org.w3c.dom.Element;

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
	//AtomicLong sz = new AtomicLong(0);
	@Override
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
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		// TODO Auto-generated method stub
		return new byte[len];
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "null";
	}

	@Override
	public void setName(String name) {
		// TODO Auto-generated method stub

	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		//return sz.get();
		return 0;
	}

	@Override
	public void deleteChunk(byte[] hash, long start, int len)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void init(Element config) {
		// TODO Auto-generated method stub

	}

	@Override
	public ChunkData getNextChunck() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void iterationInit() {
		// TODO Auto-generated method stub

	}

	@Override
	public long getFreeBlocks() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException {
		//this.sz.addAndGet(len);
		return 0;
	}

	@Override
	public long maxSize() {
		// TODO Auto-generated method stub
		return Long.MAX_VALUE;
	}

	@Override
	public long compressedSize() {
		// TODO Auto-generated method stub
		//return this.sz.get();
		return 0;
	}

	@Override
	public void deleteDuplicate(byte[] hash, long start, int len)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void sync() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setReadSpeed(int bps) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setWriteSpeed(int bps) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCacheSize(long bps) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getReadSpeed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getWriteSpeed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getMaxCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

}
