package org.opendedup.sdfs.servers;

import java.io.IOException;
import java.util.ArrayList;



import org.opendedup.collections.HashtableFullException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.network.HashClient;
import org.opendedup.sdfs.network.HashClientPool;
import org.opendedup.sdfs.notification.SDFSEvent;

public class HCServiceProxy {

	static HashClientPool p = null;
	private static HashChunkServiceInterface hcService = null;


	// private static boolean initialized = false;

	static {
		hcService = new HashChunkService();
		try {
			hcService.init();
			if (!Main.closedGracefully) {
				hcService.runConsistancyCheck();
			}
			if (Main.DSERemoteHostName != null) {
				HCServer s = new HCServer(Main.DSERemoteHostName,
						Main.DSERemotePort, false, Main.DSERemoteCompress,
						Main.DSERemoteUseSSL);
				p = new HashClientPool(s, "server", 16);
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("Unable to initialize HashChunkService ",
					e);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void processHashClaims(SDFSEvent evt) throws IOException {
		hcService.processHashClaims(evt);
	}

	public static boolean hashExists(byte[] hash, short hops)
			throws IOException, HashtableFullException {
		return hcService.hashExists(hash, hops);
	}

	public static HashChunk fetchHashChunk(byte[] hash) throws IOException {
		return hcService.fetchChunk(hash);
	}

	public static long removeStailHashes(long ms, boolean forceRun,
			SDFSEvent evt) throws IOException {
		return hcService.removeStailHashes(ms, forceRun, evt);
	}

	public static long getChunksFetched() {
		return -1;
	}

	public static synchronized void init() {
	}

	public static long getSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.hcService.getSize();
		} else {
			return -2;
		}
	}

	public static long getMaxSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.hcService.getMaxSize();
		} else {
			return -1;
		}
	}

	public static long getFreeBlocks() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.getChunkStore().getFreeBlocks();
		} else {
			return -1;
		}
	}

	public static AbstractChunkStore getChunkStore() {

		return hcService.getChuckStore();
	}

	public static int getPageSize() {
		if (Main.chunkStoreLocal) {
			return HCServiceProxy.hcService.getPageSize();
		} else {
			return -1;
		}
	}

	public static boolean writeChunk(byte[] hash, byte[] aContents,
			int position, int len, boolean sendChunk) throws IOException,
			HashtableFullException {
		boolean doop = false;
		if (Main.chunkStoreLocal) {
			// doop = HCServiceProxy.hcService.hashExists(hash);
			if (!doop && sendChunk) {
				doop = HCServiceProxy.hcService.writeChunk(hash, aContents, 0,
						Main.CHUNK_LENGTH, false);

			}
		} else {
			HashClient hc = null;
			try {
				hc = p.borrowObject();
				doop = hc.hashExists(hash, (short) 0);
				if (!doop && sendChunk) {
					try {
						hc.writeChunk(hash, aContents, 0, len);
					} catch (Exception e) {
						SDFSLogger.getLog().warn("unable to use hashclient", e);
						hc.close();
						hc.openConnection();
						hc.writeChunk(hash, aContents, 0, len);
					}
				}
			} catch (Exception e1) {
				SDFSLogger.getLog().fatal("Unable to write chunk " + hash, e1);
				throw new IOException("Unable to write chunk " + hash);
			} finally {
				if (hc != null)
					p.returnObject(hc);
			}
		}
		return doop;
	}

	public static boolean localHashExists(byte[] hash) throws IOException,
			HashtableFullException {
		boolean exists = false;
		if (Main.chunkStoreLocal) {
			exists = HCServiceProxy.hcService.localHashExists(hash);

		}
		return exists;
	}

	public static void fetchChunks(ArrayList<String> hashes, String server,
			String password, int port, boolean useSSL) throws IOException,
			HashtableFullException {
		if (Main.chunkStoreLocal) {
			HCServiceProxy.hcService.remoteFetchChunks(hashes, server,
					password, port, useSSL);
		} else {
			throw new IllegalStateException(
					"not implemented for remote chunkstores");
		}
	}

	public static boolean hashExists(byte[] hash) throws IOException,
			HashtableFullException {
		boolean exists = false;
		if (Main.chunkStoreLocal) {
			exists = HCServiceProxy.hcService.hashExists(hash, (short) 0);

		} else {
			HashClient hc = null;
			try {
				hc = p.borrowObject();
				exists = hc.hashExists(hash, (short) 0);
				if (exists) {
					// existingHashes.put(hashStr, hashStr);
				}
			} finally {
					p.returnObject(hc);
			}
		}
		return exists;
	}

	

	public static byte[] fetchChunk(byte[] hash) throws IOException {
		if (Main.chunkStoreLocal) {
			HashChunk hc = HCServiceProxy.hcService.fetchChunk(hash);
			return hc.getData();
		} else {
			
			HashClient hc = null;
			try {
				hc = p.borrowObject();
				byte[] data = hc.fetchChunk(hash);

				return data;
			}  finally {
				if (hc != null)
					p.returnObject(hc);
			}
		}

	}

	public static long getChunksRead() {
		return hcService.getChunksRead();
	}

	public static double getChunksWritten() {
		return hcService.getChunksWritten();
	}

	public static double getKBytesRead() {
		return hcService.getKBytesRead();
	}

	public static double getKBytesWrite() {
		return hcService.getKBytesWrite();
	}

	public static long getDupsFound() {
		return hcService.getDupsFound();
	}

	public static void close() {
		hcService.close();
	}

}
