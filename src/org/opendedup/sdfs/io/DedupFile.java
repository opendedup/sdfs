package org.opendedup.sdfs.io;

import java.io.IOException;

import org.opendedup.collections.HashtableFullException;

/**
 * 
 * @author annesam Interface that represents the file map associated with a
 *         specific MetaDataDedupFile. The file map stores data with a location
 *         index where each entry is a chunk of data. Locations in the index are
 *         offset by a specific chunksize
 * @see org.opendedup.sdfs.Main#CHUNK_LENGTH . Data is written the DedupFile
 *      through the DedupFileChannel
 * @see com.annesam.sdfs.io.DedupFileChannel .
 * 
 */
public interface DedupFile {
	
	public abstract void removeFromFlush(long pos);
	/**
	 * 
	 * @return true if the dedup file is closed
	 */
	public abstract boolean isClosed();

	/**
	 * Writes all the cache buffers to the dedup chunk store service
	 */
	public abstract int writeCache() throws IOException, HashtableFullException;

	/**
	 * 
	 * @param position
	 *            the position in the dedup file where the write buffer is
	 *            retrieved from
	 * @return the write buffer for the give position
	 * @throws IOException
	 */
	public abstract DedupChunkInterface getWriteBuffer(long position) throws FileClosedException,IOException;

	/**
	 * 
	 * @param position
	 *            the position within the DedupFile where to return the read
	 *            buffer from
	 * @return the specific read buffer.
	 * @throws IOException
	 */
	public abstract DedupChunkInterface getReadBuffer(long position) throws FileClosedException,IOException;
	
	public void updateMap(DedupChunkInterface writeBuffer, byte[] hash,
	boolean doop) throws FileClosedException, IOException;
	
	public void putBufferIntoFlush(DedupChunkInterface writeBuffer);


	public void updateMap(DedupChunkInterface writeBuffer, byte[] hash,
			boolean doop, boolean propigateEvent) throws FileClosedException, IOException;
	
	/**
	 * Clones the DedupFile
	 * 
	 * @param mf
	 *            the MetaDataDedupFile to clone
	 * @return the cloned DedupFile
	 * @throws IOException
	 */
	public abstract DedupFile snapshot(MetaDataDedupFile mf)
			throws IOException, HashtableFullException;


	/**
	 * Clones the DedupFile
	 * 
	 * @param mf
	 *            the MetaDataDedupFile to clone
	 * @param propigateEvent TODO
	 * @return the cloned DedupFile
	 * @throws IOException
	 */
	public abstract DedupFile snapshot(MetaDataDedupFile mf, boolean propigateEvent)
			throws IOException, HashtableFullException;
	
	/**
	 * Clones the DedupFile
	 * 
	 * @param mf
	 *            the MetaDataDedupFile to clone
	 * @return the cloned DedupFile
	 * @throws IOException
	 */
	public abstract void copyTo(String path)
			throws IOException;


	/**
	 * Clones the DedupFile
	 * @param propigateEvent TODO
	 * @param mf
	 *            the MetaDataDedupFile to clone
	 * 
	 * @return the cloned DedupFile
	 * @throws IOException
	 */
	public abstract void copyTo(String path, boolean propigateEvent)
			throws IOException;

	/**
	 * Deletes the DedupFile and all on disk references
	 * 
	 * @return true if deleted
	 */
	public abstract boolean delete();


	/**
	 * Deletes the DedupFile and all on disk references
	 * @param propigateEvent TODO
	 * 
	 * @return true if deleted
	 */
	public abstract boolean delete(boolean propigateEvent);

	/**
	 * Writes a specific cache buffer to the dedup chunk service
	 * 
	 * @param writeBuffer
	 *            the write buffer to persist
	 * @param removeWhenWritten
	 *            whether or not to remove from the cached write buffers when
	 *            written
	 * @throws IOException
	 */
	public abstract void writeCache(DedupChunkInterface writeBuffer)
			throws FileClosedException,IOException, HashtableFullException;

	/**
	 * 
	 * @return the number of chunks in the DedupFile
	 * @throws IOException
	 */
	public abstract long getNumberofChunks() throws FileClosedException,IOException;

	/**
	 * Flushes all write buffers to disk
	 * 
	 * @throws IOException
	 */
	public abstract void sync(boolean force) throws FileClosedException, IOException;


	/**
	 * Flushes all write buffers to disk
	 * @param propigateEvent TODO
	 * 
	 * @throws IOException
	 */
	public abstract void sync(boolean force,boolean propigateEvent) throws FileClosedException, IOException;

	/**
	 * Creates a DedupFileChannel for writing data to this DedupFile
	 * 
	 * @return a DedupFileChannel associated with this file
	 * @throws IOException
	 */
	public abstract DedupFileChannel getChannel(int flags) throws IOException;

	/**
	 * Removes a DedupFileChannel for writing to this DedupFile
	 * 
	 * @param channel
	 *            the channel to remove
	 */
	public abstract void unRegisterChannel(DedupFileChannel channel,int flags);
	
	public abstract void registerChannel(DedupFileChannel channel) throws IOException;


	/**
	 * 
	 * @return the path to the folder where the map for this dedup file is
	 *         located.
	 */
	public abstract String getDatabaseDirPath();

	/**
	 * Closes the DedupFile and all DedupFileChannels
	 */
	public abstract void forceClose();

	/**
	 * Gets the GUID associated with this file. Each DedupFile has an associated
	 * GUID. The GUID is typically used also as the file name of the associated
	 * Database file or on disk hashmap.
	 * 
	 * @return the GUID
	 */
	public abstract String getGUID();

	/**
	 * 
	 * @return the MetaDataDedupFile associated with this DedupFile
	 */
	public abstract MetaDataDedupFile getMetaFile();

	/**
	 * 
	 * @param lock
	 *            to remove from the file
	 */
	public abstract void removeLock(DedupFileLock lock);


	/**
	 * 
	 * @param lock
	 *            to remove from the file
	 * @param propigateEvent TODO
	 */
	public abstract void removeLock(DedupFileLock lock, boolean propigateEvent);

	/**
	 * Tries to lock a file at a specific position
	 * 
	 * @param ch
	 *            the channel that requested the lock
	 * @param position
	 *            the position to lock the file at.
	 * @param size
	 *            the size of the data to be locked
	 * @param shared
	 *            if the lock is shared or not
	 * @return true if it is locked
	 * @throws IOException
	 */
	public abstract DedupFileLock addLock(DedupFileChannel ch, long position,
			long len, boolean shared) throws IOException;


	/**
	 * Tries to lock a file at a specific position
	 * 
	 * @param ch
	 *            the channel that requested the lock
	 * @param position
	 *            the position to lock the file at.
	 * @param shared
	 *            if the lock is shared or not
	 * @param propigateEvent TODO
	 * @param size
	 *            the size of the data to be locked
	 * @return true if it is locked
	 * @throws IOException
	 */
	public abstract DedupFileLock addLock(DedupFileChannel ch, long position,
			long len, boolean shared, boolean propigateEvent) throws IOException;

	/**
	 * 
	 * @return when the file was last modified
	 */
	public abstract long lastModified() throws IOException;

	/**
	 * Returns the DedupChunk associated with a position in the DedupFile.
	 * 
	 * @param location
	 *            location to retieve. It will return the chunk where the
	 *            location sits
	 * @param create
	 *            Creates a new chunk if set to true and chunk does not exists.
	 *            If the position is empty it should return an empty DedupChunk
	 *            where the @see DedupChunk#isNewChunk() is set to true
	 * @return the DedupChunk of null if create is false and chunk is not found
	 * @throws IOException
	 */
	public abstract DedupChunkInterface getHash(long location, boolean create)
			throws IOException, FileClosedException;

	/**
	 * 
	 * @param location
	 *            the location where to remove the hash from. This is often used
	 *            when truncating a file
	 * @throws IOException
	 */
	public abstract void removeHash(long location) throws IOException;


	/**
	 * 
	 * @param location
	 *            the location where to remove the hash from. This is often used
	 *            when truncating a file
	 * @param propigateEvent TODO
	 * @throws IOException
	 */
	public abstract void removeHash(long location, boolean propigateEvent) throws IOException;

	/**
	 * 
	 * @param location
	 *            the location that is requested
	 * @return the base chunk location associated with a specific location
	 *         within a file. As an example, if location "512" is requested it
	 *         will return a chunk at location "0". If the chunk size is 4096
	 *         and location 8195 is requested it will return 8192 .
	 */
	public abstract long getChuckPosition(long location);

	/**
	 * 
	 * @return
	 */
	public abstract boolean isAbsolute();

	/**
	 * Optimizes the dedup file hash map for a specific length of file.
	 * 
	 * @param length
	 *            the lenght to optimize for
	 */
	public abstract void optimize() throws HashtableFullException;

	public abstract boolean hasOpenChannels();

	public abstract void truncate(long length) throws IOException;


	public abstract void truncate(long length, boolean propigateEvent) throws IOException;
	
	
	

}