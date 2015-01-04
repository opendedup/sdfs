package org.opendedup.collections;

import java.io.IOException;


import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.LargeBloomFilter;

public interface AbstractHashesMap {

	public abstract long endStartingPosition();

	public abstract long getSize();

	public abstract long getUsedSize();

	public abstract long getMaxSize();

	public abstract void claimRecords(SDFSEvent evt) throws IOException;

	public abstract long claimRecords(SDFSEvent evt, LargeBloomFilter bf)
			throws IOException;

	/**
	 * Searches the set for <tt>obj</tt>
	 * 
	 * @param obj
	 *            an <code>Object</code> value
	 * @return a <code>boolean</code> value
	 * @throws IOException
	 */
	public abstract boolean containsKey(byte[] key) throws IOException;

	public abstract boolean put(ChunkData cm) throws IOException,
			HashtableFullException;

	public abstract boolean put(ChunkData cm, boolean persist)
			throws IOException, HashtableFullException;

	public abstract boolean update(ChunkData cm) throws IOException;

	public abstract long get(byte[] key) throws IOException;

	public abstract byte[] getData(byte[] key) throws IOException, DataArchivedException;

	public abstract boolean remove(ChunkData cm) throws IOException;

	public abstract boolean isClaimed(ChunkData cm)
			throws KeyNotFoundException, IOException;

	public abstract long removeRecords(long ms, boolean forceRun, SDFSEvent evt)
			throws IOException;

	public abstract void sync() throws IOException;

	public abstract void close();

	public abstract void initCompact() throws IOException;

	public abstract void commitCompact(boolean force) throws IOException;

	public abstract void rollbackCompact() throws IOException;

	public abstract void init(long maxSize, String fileName)
			throws IOException, HashtableFullException;

}