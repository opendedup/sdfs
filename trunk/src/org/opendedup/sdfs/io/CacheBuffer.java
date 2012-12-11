package org.opendedup.sdfs.io;

import java.io.IOException;

/**
 * 
 * @author annesam
 * 
 *         This is client side class that is used to cache chunks for data in
 *         memory for reading or writing.
 * 
 */
public class CacheBuffer extends DedupChunk {

	private static final long serialVersionUID = 9121666684534401291L;
	// The byte array of data that is held in memory
	private byte[] chunk;
	// If the buffer has been written to
	private boolean dirty = false;
	// The end position of the written data
	private long endPosition = 0;

	/**
	 * Instantiates a CacheBuffer
	 * 
	 * @param hash
	 *            the hash associated with the cached bytes.
	 * @param startPos
	 *            the start position within the DedupFile @see DedupFile of the
	 *            data.
	 * @param length
	 *            the length of the cached byte array
	 * @param cachedBytes
	 *            the cached byte array
	 * @throws IOException
	 */
	public CacheBuffer(byte[] hash, long startPos, int length,
			byte[] cachedBytes) throws IOException {
		super(hash, startPos, length, true);
		this.chunk = cachedBytes;
		if (this.chunk.length < length)
			this.setLength(chunk.length);
		this.endPosition = this.getFilePosition() + this.getLength();
	}

	/**
	 * Instantiates a CacheBuffer based on a specific DedupChunk.
	 * 
	 * @param dk
	 *            the dedup chunk to copy to this Cachebuffer.
	 * @throws IOException
	 */
	public CacheBuffer(DedupChunk dk) throws IOException, BufferClosedException {
		super(dk.getHash(), dk.getFilePosition(), dk.getLength(), dk
				.isNewChunk());
		this.chunk = dk.getChunk();
		this.setNewChunk(dk.isNewChunk());
		this.endPosition = this.getFilePosition() + this.getLength();
	}

	/**
	 * 
	 * @return the current end position of the byte array.
	 */
	public long getEndPosition() {
		return endPosition;
	}

	/**
	 * 
	 * @param endPosition
	 *            sets the end position of the byte array
	 */
	public void setEndPosition(long endPosition) {
		this.endPosition = endPosition;
	}

	/**
	 * @return the chunk of data that is cached
	 */
	@Override
	public byte[] getChunk() {
		return chunk;
	}

	/**
	 * 
	 * @param b
	 *            the chunk of data to be cached
	 */
	public void setChunk(byte[] b) {
		this.chunk = b;
		if (this.chunk.length != this.getLength()) {
			this.setLength(chunk.length);
			this.endPosition = this.getFilePosition() + this.getLength();
		}
	}

	/**
	 * 
	 * @return true of the data is dirty
	 */
	public boolean isDirty() {
		return dirty;
	}

	/**
	 * 
	 * @param dirty
	 *            sets the data as dirty or not
	 */
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	@Override
	public String toString() {
		return this.getHash() + ":" + this.getFilePosition() + ":"
				+ this.getLength() + ":" + this.getEndPosition();
	}

}
