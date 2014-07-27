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
		return new byte[0];
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
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long maxSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long compressedSize() {
		// TODO Auto-generated method stub
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

}
