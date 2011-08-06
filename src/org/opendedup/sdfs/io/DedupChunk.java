package org.opendedup.sdfs.io;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.sdfs.servers.HCServiceProxy;

/**
 * 
 * @author annesam Base class for storing byte arrays associated with dedup
 *         files into memory.
 */
public class DedupChunk implements java.io.Serializable {
	private static final long serialVersionUID = -5440311151699047048L;
	private byte[] hash;
	private byte[] data = null;
	private int length;
	private long position;
	private boolean newChunk = false;
	private boolean writable = false;
	private boolean doop = false;
	private ReentrantLock lock = new ReentrantLock();

	public DedupChunk(long position) {
		this.position = position;
	}

	/**
	 * 
	 * @param hash
	 *            The MD5 Hash of the chunk requested.
	 * @param position
	 *            The start position within the deduplicated file
	 * @param length
	 *            The length of the chunk
	 */
	public DedupChunk(byte[] hash, long position, int length, boolean newChunk) {
		this.hash = hash;
		this.length = length;
		this.position = position;
		this.newChunk = newChunk;
		if (this.isNewChunk())
			data = new byte[this.length];
	}

	/**
	 * 
	 * @param hash
	 *            The MD5 Hash of the chunk requested.
	 * @param position
	 *            The start position within the deduplicated file
	 * @param length
	 *            The length of the chunk
	 */
	public DedupChunk(byte[] hash, byte[] data, long position, int length) {
		this.hash = hash;
		this.data = data;
		this.length = length;
		this.position = position;
		this.newChunk = false;
	}

	/**
	 * 
	 * @return returns the MD5 Hash
	 */

	public byte[] getHash() {
		return hash;
	}

	/**
	 * 
	 * @return gets the lenth of the DedupChunk
	 */
	public int getLength() {
		return length;
	}

	/**
	 * 
	 * @return the file position within the DedupFile
	 */
	public long getFilePosition() {
		return position;
	}

	/**
	 * 
	 * @param length
	 *            the length of the dedup chunk
	 */
	public void setLength(int length) {
		this.length = length;
	}

	/**
	 * 
	 * @return if this is a new chunk or one retrieved from the chunk store
	 *         service
	 */
	public boolean isNewChunk() {
		return newChunk;
	}

	/**
	 * 
	 * @param newChunk
	 *            sets the chunk as new
	 */
	public void setNewChunk(boolean newChunk) {
		this.newChunk = newChunk;
	}

	public byte[] getChunk() throws IOException, BufferClosedException {
		this.lock.lock();
		try {
			if (data != null)
				return data;
			else
				return HCServiceProxy.fetchChunk(hash);
		} finally {
			this.lock.unlock();
		}
	}

	/**
	 * sets the chunk as writable
	 * 
	 * @param writable
	 *            true if writable
	 */
	public void setWritable(boolean writable) {
		this.writable = writable;
	}

	/**
	 * 
	 * @return true if the chunk is writable
	 */

	public boolean isWritable() {
		return writable;
	}

	public void destroy() {

	}

	public void setDoop(boolean doop) {
		this.doop = doop;
	}

	public boolean isDoop() {
		return doop;
	}

	public void open() {

	}

}
