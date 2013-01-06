package org.opendedup.sdfs.servers;

import java.io.IOException;
import java.util.ArrayList;



import org.opendedup.collections.HashtableFullException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.DSECompaction;
import org.opendedup.sdfs.filestore.FileChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.filestore.HashStore;
import org.opendedup.sdfs.filestore.gc.ChunkStoreGCScheduler;
import org.opendedup.sdfs.network.HashClient;
import org.opendedup.sdfs.network.HashClientPool;
import org.opendedup.sdfs.notification.SDFSEvent;

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
	private static AbstractChunkStore fileStore = null;
	private static ChunkStoreGCScheduler csGC = null;
	private static HashClientPool hcPool = null;

	/**
	 * @return the chunksFetched
	 */
	public static long getChunksFetched() {
		return chunksFetched;

	}

	static {
		try {
			fileStore = (AbstractChunkStore) Class
					.forName(Main.chunkStoreClass).newInstance();
			fileStore.init(Main.chunkStoreConfig);
		} catch (InstantiationException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			System.exit(-1);
		} catch (IllegalAccessException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			System.exit(-1);
		} catch (ClassNotFoundException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			System.exit(-1);
		} catch (IOException e) {
			SDFSLogger.getLog().fatal("Unable to initiate ChunkStore", e);
			System.exit(-1);
		}
		try {
			hs = new HashStore();
			
			if (!Main.chunkStoreLocal && Main.enableNetworkChunkStore) {
				csGC = new ChunkStoreGCScheduler();
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to start hashstore", e);
			System.exit(-1);
		}
		if (Main.upStreamDSEHostEnabled) {
			try {
				hcPool = new HashClientPool(new HCServer(
						Main.upStreamDSEHostName, Main.upStreamDSEPort, false,
						false,Main.serverUseSSL), "upstream", 24);
			} catch (IOException e) {
				System.err.println("warning unable to connect to upstream server " + Main.upStreamDSEHostName + ":" +Main.upStreamDSEPort);
				SDFSLogger.getLog().error("warning unable to connect to upstream server " + Main.upStreamDSEHostName + ":" +Main.upStreamDSEPort, e);
				System.err.println("Disabling upstream host " + Main.upStreamDSEHostName + ":" +Main.upStreamDSEPort);
				Main.upStreamDSEHostEnabled = false;
				SDFSLogger.getLog().warn("Disabling upstream host " + Main.upStreamDSEHostName + ":" +Main.upStreamDSEPort);
				//e.printStackTrace();
				//System.exit(-1);
			}
		}
	}

	private static long dupsFound;

	public static AbstractChunkStore getChuckStore() {
		return fileStore;
	}

	public static boolean writeChunk(byte[] hash, byte[] aContents,
			int position, int len, boolean compressed) throws IOException,
			HashtableFullException {
		if (aContents.length > Main.chunkStorePageSize)
			throw new IOException("content size out of bounds ["
					+ aContents.length + "] > [" + Main.chunkStorePageSize
					+ "]");
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
	
	public static boolean localHashExists(byte[] hash) throws IOException {
		return hs.hashExists(hash);
	}
	
	public static void remoteFetchChunks(ArrayList<String> al,String server,String password,int port,boolean useSSL) throws IOException, HashtableFullException {
			HCServer hserver = new HCServer(server,port,false,false,useSSL);
			HashClient hc = new HashClient(hserver,"replication",password);
			try {
				ArrayList<HashChunk> hck = hc.fetchChunks(al);
				for(int i=0;i<hck.size();i++) {
					HashChunk _hc = hck.get(i);
					writeChunk(_hc.getName(), _hc.getData(), 0, _hc.getData().length, false);
				}
			} finally {
				hc.close();
			}
	}

	public static boolean hashExists(byte[] hash, short hops)
			throws IOException, HashtableFullException {
		boolean exists = hs.hashExists(hash);
		if (hops < Main.maxUpStreamDSEHops) {
			if (!exists && Main.upStreamDSEHostEnabled) {
				HashClient hc = null;
				try {
					hc = hcPool.borrowObject();

					exists = hc.hashExists(hash, hops++);
					if (exists) {
						byte[] b = hc.fetchChunk(hash);
						writeChunk(hash, b, 0, hash.length, false);
					}
				} finally {
					hcPool.returnObject(hc);
				}
			}
			
		}else {
			SDFSLogger.getLog().info("hops reached " + hops);
		}
		return exists;
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

	public static void processHashClaims(SDFSEvent evt) throws IOException {
		hs.processHashClaims(evt);
	}

	public static long removeStailHashes(long ms, boolean forceRun,SDFSEvent evt)
			throws IOException {
		return hs.evictChunks(ms, forceRun,evt);
	}

	public static void commitChunks() {
		// H2HashStore.commitTransactions();
		unComittedChunks = 0;
	}

	public static long getSize() {
		return hs.getEntries();
	}

	public static long getFreeBlocks() {
		return hs.getFreeBlocks();
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
		fileStore.close();
		if (csGC != null)
			csGC.stopSchedules();
		hs.close();

	}

	public static void init() throws IOException {
		if(Main.runCompact) {
			DSECompaction.runCheck(hs.bdb,(FileChunkStore)HashChunkService.getChuckStore());
			SDFSLogger.getLog().info("Finished compaction");
			
		}
	}

}
