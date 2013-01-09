package org.opendedup.sdfs.servers;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.HashtableFullException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.AbstractChunkStore;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.network.HashClient;
import org.opendedup.sdfs.network.HashClientPool;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.StringUtils;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;

public class HCServiceProxy {

	public static HashMap<String, HashClientPool> dseServers = new HashMap<String, HashClientPool>();
	public static HashMap<String, HashClientPool> dseRoutes = new HashMap<String, HashClientPool>();
	private static HashChunkServiceInterface hcService = null;
	private static int cacheLenth = 10485760 / Main.CHUNK_LENGTH;
	private static ConcurrentLinkedHashMap<String, ByteCache> readBuffers = new Builder<String, ByteCache>()
			.concurrencyLevel(Main.writeThreads).initialCapacity(cacheLenth)
			.maximumWeightedCapacity(cacheLenth)
			.listener(new EvictionListener<String, ByteCache>() {
				// This method is called just after a new entry has been
				// added
				@Override
				public void onEviction(String key, ByteCache writeBuffer) {
				}
			}

			).build();

	private static HashMap<String, byte[]> readingBuffers = new HashMap<String, byte[]>();
	// private static LRUMap existingHashes = new
	// LRUMap(Main.systemReadCacheSize);
	private static ReentrantLock readlock = new ReentrantLock();

	// private static boolean initialized = false;
	
	static {
		hcService = new HashChunkService();
		try {
		hcService.init();
		}catch(Exception e) {
			SDFSLogger.getLog().error("Unable to initialize HashChunkService ",e);
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
			return HCServiceProxy.hcService.getFreeBlocks();
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

	private static HashClient getReadHashClient(String name) throws Exception {
		HashClient hc = dseRoutes.get(name).borrowObject();
		return hc;
	}

	private static void returnObject(String name, HashClient hc)
			throws IOException {
		dseRoutes.get(name).returnObject(hc);
	}

	private static HashClient getWriteHashClient(String name) throws Exception {
		HashClient hc = dseRoutes.get(name).borrowObject();
		return hc;
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
			byte[] hashRoute = { hash[0] };
			String db = StringUtils.getHexString(hashRoute);
			HashClient hc = null;
			try {
				hc = getWriteHashClient(db);
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
				// TODO Auto-generated catch block
				SDFSLogger.getLog().fatal("Unable to write chunk " + hash, e1);
				throw new IOException("Unable to write chunk " + hash);
			} finally {
				if (hc != null)
					returnObject(db, hc);
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
			String hashStr = StringUtils.getHexString(hash);
			if (readBuffers.containsKey(hashStr)) {
				return true;
			}
			/*
			 * if (existingHashes.containsKey(hashStr)) { return true; }
			 */
			String db = null;
			HashClient hc = null;
			try {
				byte[] hashRoute = { hash[0] };
				db = StringUtils.getHexString(hashRoute);
				hc = getWriteHashClient(db);
			} catch (Exception e1) {
				SDFSLogger.getLog().fatal(
						"unable to execute find hash for " + hashStr);
				throw new IOException(e1);
			}
			try {
				exists = hc.hashExists(hash, (short) 0);
				if (exists) {
					// existingHashes.put(hashStr, hashStr);
				}
			} catch (IOException e) {
				throw new IOException(e);
			} finally {
				try {
					returnObject(db, hc);
				} catch (Exception e) {
					SDFSLogger
							.getLog()
							.fatal("unable to return network thread object to pool",
									e);
				}
			}
		}
		return exists;
	}

	public static boolean cacheContains(String hashStr) {
		return readBuffers.containsKey(hashStr);
	}

	public static byte[] fetchChunk(byte[] hash) throws IOException {
		if (Main.chunkStoreLocal) {
			HashChunk hc = HCServiceProxy.hcService.fetchChunk(hash);
			return hc.getData();
		} else {
			String hashStr = StringUtils.getHexString(hash);
			boolean reading = false;
			ByteCache cache = null;
			try {
				cache = readBuffers.get(hashStr);
				if (cache != null) {
					return cache.getCache();
				}
			} catch (Exception e) {
			}

			try {
				readlock.lock();
				reading = readingBuffers.containsKey(hashStr);
				if (!reading) {
					readingBuffers.put(hashStr, hash);
				}
			} catch (Exception e) {
			} finally {
				readlock.unlock();
			}
			if (reading) {
				int z = 0;
				while (readingBuffers.containsKey(hashStr)) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						break;
					}
					z++;
					if (readBuffers.containsKey(hashStr)) {
						readingBuffers.remove(hashStr);
						break;
					} else if (z > Main.multiReadTimeout) {
						if (Main.multiReadTimeout > 0)
							SDFSLogger.getLog().info(
									"Timeout waiting for read " + hashStr);
						readingBuffers.remove(hashStr);
						break;
					}
				}
			}
			try {
				cache = readBuffers.get(hashStr);
				if (cache != null) {
					readingBuffers.remove(hashStr);
					return cache.getCache();
				}
			} catch (Exception e) {
			}
			String db = null;
			HashClient hc = null;
			try {
				byte[] hashRoute = { hash[0] };
				db = StringUtils.getHexString(hashRoute);
				hc = getReadHashClient(db);
				byte[] data = hc.fetchChunk(hash);
				ByteCache _b = new ByteCache(data);
				readlock.lock();
				readBuffers.put(hashStr, _b);
				readingBuffers.remove(hashStr);
				// kBytesFetched = kBytesFetched + (data.length / KBYTE);
				// chunksFetched++;

				return data;
			} catch (Exception e) {
				SDFSLogger.getLog()
						.warn("Unable to fetch buffer " + hashStr, e);
				throw new IOException("Unable to fetch buffer " + hashStr);
			} finally {
				if (hc != null)
					returnObject(db, hc);
				if (readlock.isLocked())
					readlock.unlock();
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
