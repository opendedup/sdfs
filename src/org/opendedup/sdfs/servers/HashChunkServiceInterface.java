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
package org.opendedup.sdfs.servers;

import java.io.IOException;


import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.collections.InsertRecord;
import org.opendedup.hashing.LargeBloomFilter;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.filestore.cloud.RemoteVolumeInfo;
import org.opendedup.sdfs.notification.SDFSEvent;

public interface HashChunkServiceInterface {

	/**
	 * @return the chunksFetched
	 */
	public abstract String restoreBlock(byte[] hash,long id) throws IOException;

	public abstract boolean blockRestored(String id) throws IOException;
	
	public abstract boolean mightContainKey(byte [] key,long id);

	public abstract long getChunksFetched();

	public abstract AbstractChunkStore getChuckStore();
	
	public abstract boolean claimKey(byte [] key,long val,long ct) throws IOException;
	

	public abstract InsertRecord writeChunk(byte[] hash, byte[] aContents,
			boolean compressed,long ct,String uuid) throws IOException, HashtableFullException;

	

	public abstract long hashExists(byte[] hash) throws IOException,
			HashtableFullException;

	public abstract HashChunk fetchChunk(byte[] hash,long pos) throws IOException,
			DataArchivedException;

	public abstract void cacheChunk(long pos) throws IOException,
			DataArchivedException;

	public abstract byte getHashRoute(byte[] hash);

	public abstract long processHashClaims(SDFSEvent evt,boolean compact) throws IOException;

	public abstract long processHashClaims(SDFSEvent evt, LargeBloomFilter bf)
			throws IOException;

	public abstract void commitChunks();

	public abstract AbstractHashesMap getHashesMap();

	public abstract void runConsistancyCheck();

	public abstract long getSize();

	public abstract long getMaxSize();

	public abstract int getPageSize();

	public abstract long getChunksRead();

	public abstract long getChunksWritten();

	public abstract double getKBytesRead();

	public abstract double getKBytesWrite();

	public abstract long getDupsFound();

	public abstract void close();

	public abstract void sync() throws IOException;

	public abstract void init() throws IOException;

	public abstract void setReadSpeed(int speed);

	public abstract void setWriteSpeed(int speed);

	public abstract long getCacheSize();

	public abstract long getMaxCacheSize();

	public abstract int getReadSpeed();

	public abstract int getWriteSpeed();

	public abstract void setCacheSize(long sz) throws IOException;
	
	public abstract void setDseSize(long sz) throws IOException;

	public abstract RemoteVolumeInfo[] getConnectedVolumes() throws IOException;

	void clearRefMap() throws IOException;

}
