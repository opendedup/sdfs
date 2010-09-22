package org.opendedup.sdfs.servers;

import java.io.IOException;
import org.opendedup.util.SDFSLogger;


import org.opendedup.collections.HashtableFullException;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.filestore.HashStore;
import org.opendedup.sdfs.filestore.gc.ChunkStoreGCScheduler;

public class HashChunkService {

	private static double kBytesRead;
	private static double kBytesWrite;
	private static final long KBYTE = 1024L;
	private static long chunksRead;
	private static long chunksWritten;
	private static long chunksFetched;
	private static double kBytesFetched;
	private static int unComittedChunks;
	private static int MAX_UNCOMITTEDCHUNKS = 100;
	private static HashStore hs = null;
	
	private static ChunkStoreGCScheduler csGC = null;

	/**
	 * @return the chunksFetched
	 */
	public static long getChunksFetched() {
		return chunksFetched;
	}

	static {
		try {
			hs = new HashStore();
			csGC = new ChunkStoreGCScheduler();
		} catch (Exception e) {
			SDFSLogger.getLog().fatal( "unable to start hashstore", e);
			System.exit(-1);
		}
	}

	private static long dupsFound;

	public static boolean writeChunk(byte[] hash, byte[] aContents,
			int position, int len, boolean compressed) throws IOException, HashtableFullException {
		if(aContents.length > Main.chunkStorePageSize)
			throw new IOException("content size out of bounds [" +aContents.length + "] > [" + Main.chunkStorePageSize + "]");
		chunksRead++;
		kBytesRead = kBytesRead + (position / KBYTE);
		boolean written = hs.addHashChunk(new HashChunk(hash, 0, len,
				aContents, compressed));
		if (written) {
			unComittedChunks++;
			chunksWritten++;
			kBytesWrite = kBytesWrite + (position / KBYTE);
			if (unComittedChunks > MAX_UNCOMITTEDCHUNKS) {
				commitChunks();
			}
			return false;
		} else {
			dupsFound++;
			return true;
		}
	}

	public static boolean hashExists(byte[] hash) throws IOException {
		return hs.hashExists(hash);
	}

	public static HashChunk fetchChunk(byte[] hash) throws IOException {
		HashChunk hashChunk = hs.getHashChunk(hash);
		byte[] data = hashChunk.getData();
		kBytesFetched = kBytesFetched + (data.length / KBYTE);
		chunksFetched++;
		return hashChunk;
	}

	public static byte getHashRoute(byte[] hash) {
		byte hashRoute = (byte) (hash[1] / (byte) 16);
		if (hashRoute < 0) {
			hashRoute += 1;
			hashRoute *= -1;
		}
		return hashRoute;
	}

	public static void processHashClaims() throws IOException {
		hs.processHashClaims();
	}

	public static void removeStailHashes() throws IOException {
		hs.evictChunks(System.currentTimeMillis() - Main.evictionAge*60*60*1000);
	}
	public static void removeStailHashes(int minutes) throws IOException {
		hs.evictChunks(System.currentTimeMillis() - minutes*60*1000);
	}

	public static void commitChunks() {
		// H2HashStore.commitTransactions();
		unComittedChunks = 0;
	}
	
	public static long getSize() {
		return hs.getEntries();
	}
	
	public static long getMaxSize() {
		return hs.getMaxEntries();
	}
	
	public static int getPageSize() {
		return Main.chunkStorePageSize;
	}

	public static long getChunksRead() {
		return chunksRead;
	}

	public static long getChunksWritten() {
		return chunksWritten;
	}

	public static double getKBytesRead() {
		return kBytesRead;
	}

	public static double getKBytesWrite() {
		return kBytesWrite;
	}

	public static long getDupsFound() {
		return dupsFound;
	}

	public static void close() {
		csGC.stopSchedules();
		hs.close();

	}

	public static void init() {

	}

}
