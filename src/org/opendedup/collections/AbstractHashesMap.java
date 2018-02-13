/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.collections;

import java.io.IOException;


import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.notification.SDFSEvent;

public interface AbstractHashesMap {

	public abstract long endStartingPosition();

	public abstract long getSize();

	public abstract long getUsedSize();

	public abstract long getMaxSize();
	
	public abstract void setMaxSize(long sz) throws IOException;

	public abstract boolean mightContainKey(byte [] key,long id);

	public abstract long claimRecords(SDFSEvent evt,boolean compact) throws IOException;

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

	public abstract InsertRecord put(ChunkData cm) throws IOException,
			HashtableFullException;

	public abstract InsertRecord put(ChunkData cm, boolean persist)
			throws IOException, HashtableFullException;

	public abstract boolean update(ChunkData cm) throws IOException;

	public abstract void cache(long pos) throws IOException, DataArchivedException;

	public abstract long get(byte[] key) throws IOException;

	public abstract byte[] getData(byte[] key) throws IOException,
			DataArchivedException;
	
	public abstract byte[] getData(byte[] key,long pos) throws IOException,
	DataArchivedException;

	public abstract boolean remove(ChunkData cm) throws IOException;

	public abstract boolean isClaimed(ChunkData cm)
			throws KeyNotFoundException, IOException;

	public abstract void sync() throws IOException;

	public abstract void close();

	public abstract void initCompact() throws IOException;

	public abstract void commitCompact(boolean force) throws IOException;

	public abstract void rollbackCompact() throws IOException;

	public abstract void init(long maxSize, String fileName, double fpp)
			throws IOException, HashtableFullException;

	void clearRefMap() throws IOException;

	boolean claimKey(byte[] hash, long val, long ct) throws IOException;
	
}
