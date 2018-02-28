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
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.ConsistancyCheck;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.filestore.HashStore;
import org.opendedup.sdfs.filestore.cloud.AbstractCloudFileSync;
import org.opendedup.sdfs.filestore.cloud.RemoteVolumeInfo;
import org.opendedup.sdfs.notification.SDFSEvent;

public class HashChunkService implements HashChunkServiceInterface {

	private double kBytesRead;
	private double kBytesWrite;
	private final long KBYTE = 1024L;
	private long chunksRead;
	private long chunksWritten;
	private long chunksFetched;
	private double kBytesFetched;
	private int unComittedChunks;
	private int MAX_UNCOMITTEDCHUNKS = 100;
	private HashStore hs = null;
	private AbstractChunkStore fileStore = null;

	// private HashClientPool hcPool = null;

	/**
	 * @return the chunksFetched
	 */
	public long getChunksFetched() {
		return chunksFetched;

	}

	public HashChunkService() {
		try {
			fileStore = (AbstractChunkStore) Class
					.forName(Main.chunkStoreClass).newInstance();
			fileStore.init(Main.chunkStoreConfig);
		} catch (Throwable e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			System.err.println("Unable to initiate ChunkStore");
			e.printStackTrace();
			System.exit(-1);
		}
		try {
			hs = new HashStore(this);
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to start hashstore", e);
			System.err.println("Unable to initiate hashstore");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private long dupsFound;

	public AbstractChunkStore getChuckStore() {
		return fileStore;
	}

	public AbstractHashesMap getHashesMap() {
		return hs.bdb;
	}

	public InsertRecord writeChunk(byte[] hash, byte[] aContents,
			boolean compressed,long ct,String uuid) throws IOException, HashtableFullException {
		if (aContents.length > Main.chunkStorePageSize)
			throw new IOException("content size out of bounds ["
					+ aContents.length + "] > [" + Main.chunkStorePageSize
					+ "]");
		chunksRead++;
		InsertRecord written = hs.addHashChunk(new HashChunk(hash, aContents,
				compressed,ct,uuid));
		if (written.getInserted()) {
			unComittedChunks++;
			chunksWritten++;
			kBytesWrite = kBytesWrite + (aContents.length / KBYTE);
			if (unComittedChunks > MAX_UNCOMITTEDCHUNKS) {
				commitChunks();
			}
		} else {
			dupsFound++;
		}
		return written;
	}
	@Override
	public void clearRefMap() throws IOException {
		hs.clearRefMap();
	}

	public void setReadSpeed(int speed) {
		fileStore.setReadSpeed((int) speed);
	}

	public void setWriteSpeed(int speed) {
		fileStore.setWriteSpeed((int) speed);
	}

	public void setCacheSize(long sz) throws IOException {
		fileStore.setCacheSize(sz);
	}

	public void setDseSize(long sz) throws IOException {
		hs.bdb.setMaxSize(sz);
	}

	

	public long hashExists(byte[] hash) throws IOException,
			HashtableFullException {
		return hs.hashExists(hash);
	}

	public HashChunk fetchChunk(byte[] hash,long pos) throws IOException,
			DataArchivedException {
		HashChunk hashChunk = hs.getHashChunk(hash,pos);
		byte[] data = hashChunk.getData();
		kBytesFetched = kBytesFetched + (data.length / KBYTE);
		chunksFetched++;
		this.kBytesRead = kBytesFetched;
		this.chunksRead = this.chunksFetched;
		return hashChunk;
	}

	public void cacheChunk(long pos) throws IOException,
			DataArchivedException {
		hs.cacheChunk(pos);
	}

	public byte getHashRoute(byte[] hash) {
		byte hashRoute = (byte) (hash[1] / (byte) 16);
		if (hashRoute < 0) {
			hashRoute += 1;
			hashRoute *= -1;
		}
		return hashRoute;
	}

	public long processHashClaims(SDFSEvent evt,boolean compact) throws IOException {
		return hs.processHashClaims(evt,compact);
	}

	public long processHashClaims(SDFSEvent evt, LargeBloomFilter bf)
			throws IOException {
		return hs.processHashClaims(evt, bf);
	}

	public void commitChunks() {
		// H2HashStore.commitTransactions();
		unComittedChunks = 0;
	}

	public long getSize() {
		return hs.getEntries();
	}

	public long getMaxSize() {
		return hs.getMaxEntries();
	}

	public int getPageSize() {
		return Main.chunkStorePageSize;
	}

	public long getChunksRead() {
		return chunksRead;
	}

	public long getChunksWritten() {
		return chunksWritten;
	}

	public double getKBytesRead() {
		return kBytesRead;
	}

	public double getKBytesWrite() {
		return kBytesWrite;
	}

	public long getDupsFound() {
		return dupsFound;
	}

	public void close() {
		SDFSLogger.getLog().info("Closing Block Store");
		fileStore.close();
		SDFSLogger.getLog().info("Closing Hash Store");
		hs.close();
	}

	public void init() throws IOException {
		
	}

	@Override
	public void runConsistancyCheck() {
		SDFSLogger.getLog().info(
				"DSE did not close gracefully, running consistancy check");
		ConsistancyCheck.runCheck(hs.bdb, getChuckStore());

	}

	@Override
	public void sync() throws IOException {
		fileStore.sync();

	}

	@Override
	public long getCacheSize() {
		return fileStore.getCacheSize();
	}

	@Override
	public long getMaxCacheSize() {
		return fileStore.getMaxCacheSize();
	}

	@Override
	public int getReadSpeed() {
		return fileStore.getReadSpeed();
	}

	@Override
	public int getWriteSpeed() {
		return fileStore.getWriteSpeed();
	}

	@Override
	public String restoreBlock(byte[] hash,long id) throws IOException {
		return hs.restoreBlock(hash,id);
	}

	@Override
	public boolean blockRestored(String id) throws IOException {
		return hs.blockRestored(id);
	}

	@Override
	public RemoteVolumeInfo[] getConnectedVolumes() throws IOException {
		if (fileStore instanceof AbstractCloudFileSync) {
			AbstractCloudFileSync af = (AbstractCloudFileSync)fileStore;
			return af.getConnectedVolumes();
		} else
			return null;
	}

	@Override
	public boolean mightContainKey(byte[] key,long id) {
		return hs.mightContainKey(key,id);
	}

	@Override
	public boolean claimKey(byte[] key,long val,long ct) throws IOException {
		return hs.claimKey(key,val,ct);
	}

}
