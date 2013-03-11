package org.opendedup.sdfs.io;

import java.io.IOException;

public interface DedupChunkInterface {

	/**
	 * 
	 * @return returns the MD5 Hash
	 */

	public abstract byte[] getHash();

	/**
	 * 
	 * @return gets the lenth of the DedupChunk
	 */
	public abstract int getLength();

	/**
	 * 
	 * @return the file position within the DedupFile
	 */
	public abstract long getFilePosition();

	/**
	 * 
	 * @param length
	 *            the length of the dedup chunk
	 */
	public abstract void setLength(int length);

	/**
	 * 
	 * @return if this is a new chunk or one retrieved from the chunk store
	 *         service
	 */
	public abstract boolean isNewChunk();

	/**
	 * 
	 * @param newChunk
	 *            sets the chunk as new
	 */
	public abstract void setNewChunk(boolean newChunk);

	public abstract byte[] getChunk() throws IOException, BufferClosedException;

	/**
	 * sets the chunk as writable
	 * 
	 * @param writable
	 *            true if writable
	 */
	public abstract void setWritable(boolean writable);

	/**
	 * 
	 * @return true if the chunk is writable
	 */

	public abstract boolean isWritable();

	public abstract void destroy();

	public abstract void setDoop(boolean doop);

	public abstract boolean isDoop();

	
	public abstract int getBytesWritten();

	public abstract DedupFile getDedupFile();

	public abstract boolean sync() throws IOException;

	public abstract int capacity();

	public abstract long getEndPosition();
	
	public byte[] getFlushedBuffer() throws BufferClosedException;
	
	public boolean isClosed();
	
	public void flush() throws BufferClosedException;

	/**
	 * Writes to the given target array
	 * 
	 * @param b
	 *            the source array
	 * @param pos
	 *            the position within the target array to write to
	 * @param len
	 *            the length to write from the target array
	 * @throws BufferClosedException
	 * @throws IOException
	 */

	public abstract void write(byte[] b, int pos) throws BufferClosedException,
			IOException;

	public abstract void truncate(int len) throws BufferClosedException;

	public abstract boolean isDirty();

	public abstract void setDirty(boolean dirty);

	public abstract String toString();

	public abstract void open();

	public abstract void close() throws IOException;

	public abstract void persist();


	public abstract boolean isPrevDoop();

	public abstract void setPrevDoop(boolean prevDoop);

	public abstract int hashCode();



}