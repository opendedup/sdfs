package org.opendedup.sdfs.filestore;

import java.io.IOException;

import org.w3c.dom.Element;

/**
 * 
 * @author Sam Silverberg The AbstractChunkStore is used as in interface to
 *         store deduped chunks of data from clients to disk or some other
 *         storage device or service for later retrieval. Within the chunk store
 *         service itself data passes as follows
 * 
 *         client -> TCP -> Client Thread ->HashChunkService -> TCHashStore ->
 *         AbstactChunkStore
 * 
 * @see FileChunkStore
 * @see S3ChunkStore
 * @see NullChunkStore
 * @see TCChunkStore
 * 
 */

public interface AbstractChunkStore {

	public abstract long getFreeBlocks();

	/**
	 * Closes the chunk store, if this is required
	 */
	public abstract void close();

	/**
	 * Closes the chunk store, if this is required
	 */
	public abstract void init(Element config) throws IOException;

	/**
	 * Each chunk store must have a unique name.
	 * 
	 * @return returns the unique name of the chunk store
	 */
	public abstract String getName();

	/**
	 * Sets the name of the chunk store
	 * 
	 * @param name
	 *            the name of the chunk store.
	 */
	public abstract void setName(String name);

	/**
	 * 
	 * @return Returns the size of the chunk store on disk or service
	 */
	public abstract long size();

	/**
	 * 
	 * @return The size of the chunkstore compressed
	 */
	public abstract long compressedSize();

	public abstract long maxSize();

	/**
	 * 
	 * @return bytes read since chunk store was started
	 */
	public abstract long bytesRead();

	/**
	 * 
	 * @return returns bytes Written since chunk store was started
	 */

	public abstract long bytesWritten();

	/**
	 * reserves a write position within the chunk store for later use. If
	 * implemented but unused this should return 0
	 * 
	 * @param len
	 *            the length of the chunk that will be stored
	 * @return the position with the chunk store where the chunk will be written
	 * @throws IOException
	 */
	// public abstract long reserveWritePosition(int len) throws IOException;

	/**
	 * writes the chunk to the chunk store
	 * 
	 * @param hash
	 *            the unique (md5 or SHA) hash for the data stored.
	 * @param chunk
	 *            the actual data to be stored
	 * @param len
	 *            the length of the data to be stored
	 * @param start
	 *            the position within the chunk store that the chunk should be
	 *            stored at
	 * @throws IOException
	 */
	public abstract long writeChunk(byte[] hash, byte[] chunk, int len)
			throws IOException;

	/**
	 * gets a chunk of data from the chunk store
	 * 
	 * @param hash
	 *            hash the unique (md5 or SHA) hash for the data requested
	 * @param start
	 *            the position within the chunk store that the chunk should be
	 *            located
	 * @param len
	 *            the length of the data
	 * @return the chunk of data requested
	 * @throws IOException
	 */
	public abstract byte[] getChunk(byte[] hash, long start, int len)
			throws IOException;

	/**
	 * tunes the chunk store to a specific size
	 * 
	 * @param length
	 *            the space that should be allocated on disk for this chunk
	 *            store.
	 * @throws IOException
	 */

	public abstract void deleteChunk(byte[] hash, long start, int len)
			throws IOException;

	public abstract void deleteDuplicate(byte[] hash, long start, int len)
			throws IOException;

	public abstract ChunkData getNextChunck() throws IOException;

	public abstract void iterationInit() throws IOException;

}