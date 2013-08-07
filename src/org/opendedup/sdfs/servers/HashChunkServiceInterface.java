package org.opendedup.sdfs.servers;

import java.io.IOException;
import java.util.ArrayList;

import org.opendedup.collections.HashtableFullException;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.notification.SDFSEvent;

public interface HashChunkServiceInterface {

	/**
	 * @return the chunksFetched
	 */
	public abstract long getChunksFetched();

	public abstract AbstractChunkStore getChuckStore();

	public abstract boolean writeChunk(byte[] hash, byte[] aContents,
			int position, int len, boolean compressed) throws IOException,
			HashtableFullException;

	public abstract boolean localHashExists(byte[] hash) throws IOException;

	public abstract void remoteFetchChunks(ArrayList<String> al, String server,
			String password, int port, boolean useSSL) throws IOException,
			HashtableFullException;

	public abstract boolean hashExists(byte[] hash)
			throws IOException, HashtableFullException;

	public abstract HashChunk fetchChunk(byte[] hash) throws IOException;

	public abstract byte getHashRoute(byte[] hash);

	public abstract void processHashClaims(SDFSEvent evt) throws IOException;

	public abstract long removeStailHashes(long ms, boolean forceRun,
			SDFSEvent evt) throws IOException;

	public abstract void commitChunks();
	
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

	public abstract void init() throws IOException;

}