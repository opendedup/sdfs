package org.opendedup.sdfs.servers;

import java.io.IOException;
import java.util.ArrayList;

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.collections.DataArchivedException;
import org.opendedup.collections.HashtableFullException;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.LargeBloomFilter;


public interface HashChunkServiceInterface {

	/**
	 * @return the chunksFetched
	 */
	public abstract String restoreBlock(byte [] hash)throws IOException;
	
	public abstract boolean blockRestored(String id)throws IOException;
	
	public abstract long getChunksFetched();

	public abstract AbstractChunkStore getChuckStore();

	public abstract boolean writeChunk(byte[] hash, byte[] aContents, boolean compressed) throws IOException,
			HashtableFullException;

	public abstract void remoteFetchChunks(ArrayList<String> al, String server,
			String password, int port, boolean useSSL) throws IOException,
			HashtableFullException;

	public abstract boolean hashExists(byte[] hash) throws IOException,
			HashtableFullException;

	public abstract HashChunk fetchChunk(byte[] hash) throws IOException,DataArchivedException;

	public abstract byte getHashRoute(byte[] hash);

	public abstract void processHashClaims(SDFSEvent evt) throws IOException;

	public abstract long processHashClaims(SDFSEvent evt,
			LargeBloomFilter bf) throws IOException;

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
	
	public abstract void setReadSpeed(int speed) ;
	
	public abstract void setWriteSpeed(int speed) ;
	
	public abstract long getCacheSize();
	
	public abstract long getMaxCacheSize();
	
	public abstract int getReadSpeed();
	
	public abstract int getWriteSpeed();
	
	public abstract void setCacheSize(long sz) throws IOException;

}