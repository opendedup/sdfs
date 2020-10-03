package org.opendedup.sdfs.filestore;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.sdfs.Main;
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
	// AtomicLong sz = new AtomicLong(0);
	
	AtomicLong sz = new AtomicLong();
	@Override
	public long bytesRead() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long bytesWritten() {
		// TODO Auto-generated method stub
		return sz.get();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] getChunk(byte[] hash, long start, int len) throws IOException {
		return new byte[len];
	}

	@Override
	public String getName() {
		return "null";
	}

	@Override
	public void setName(String name) {

	}

	@Override
	public long size() {
		return sz.get();
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
	public void iterationInit(boolean deep) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getFreeBlocks() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long writeChunk(byte[] hash, byte[] chunk, int len,String uuid)
			throws IOException {
		this.sz.addAndGet(chunk.length);
		return 0;
	}

	@Override
	public long maxSize() {
		// TODO Auto-generated method stub
		return Main.chunkStoreAllocationSize;
	}

	@Override
	public long compressedSize() {
		// TODO Auto-generated method stub
		return this.sz.get();
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
	public void setDseSize(long bps) {
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

	@Override
	public String restoreBlock(long id, byte[] hash) {
		return null;

	}

	@Override
	public boolean blockRestored(String id) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void deleteStore() {
		// TODO Auto-generated method stub

	}

	@Override
	public void compact() {
		// TODO Auto-generated method stub

	}

	@Override
	public void cacheData(long len) throws IOException,
			DataArchivedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void clearCounters() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getAllObjSummary(String pp, long id) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
