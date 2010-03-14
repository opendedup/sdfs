package org.opendedup.sdfs.servers;

import java.io.IOException;



import java.util.logging.Logger;

import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.filestore.MemoryHashStore;



public class HashChunkService {
	
	private static double kBytesRead;
	private static double kBytesWrite;
	private static final long KBYTE = 1024L;
	private static long chunksRead;
	private static long chunksWritten;
	private static long chunksFetched;
	private static double kBytesFetched;
	private static int unComittedChunks;
	private static int MAX_UNCOMITTEDCHUNKS=100;
	/**
	 * @return the chunksFetched
	 */
	public static long getChunksFetched() {
		return chunksFetched;
	}
	

	private static long dupsFound;
	
	public static boolean writeChunk(byte[] hash, byte[] aContents, int position,int len,boolean compressed) throws IOException{
		chunksRead++;
		kBytesRead = kBytesRead + (position / KBYTE);
		boolean written = MemoryHashStore.addHash(getHashRoute(hash), hash, 0, position,aContents,compressed);
		if(written) {
			unComittedChunks++;
			chunksWritten++;
			kBytesWrite = kBytesWrite + (position / KBYTE);
			if(unComittedChunks > MAX_UNCOMITTEDCHUNKS) {
				commitChunks();
			}
			return false;
		} else {
			dupsFound++;
			return true;
		}
	}
	
	public static boolean hashExists(byte[] hash) throws IOException {
		return MemoryHashStore.hashExists(getHashRoute(hash), hash);
	}
	
	public static HashChunk fetchChunk(byte[] hash) throws IOException {
		HashChunk hashChunk = MemoryHashStore.getHashChunk(getHashRoute(hash), hash);
		byte [] data = hashChunk.getData();
		kBytesFetched = kBytesFetched + (data.length / KBYTE);
		chunksFetched++;
		return hashChunk;
	}
	
	public static byte getHashRoute(byte[] hash) {
		byte hashRoute = (byte)(hash[1]/(byte)16);
		if(hashRoute < 0) {
			hashRoute += 1;
			hashRoute *= -1;
		}
		return hashRoute;
	}
	
	public static void commitChunks() {
		//H2HashStore.commitTransactions();
		unComittedChunks = 0;
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

}
