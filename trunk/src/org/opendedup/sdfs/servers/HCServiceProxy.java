package org.opendedup.sdfs.servers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.util.logging.*;

import org.apache.commons.collections.map.LRUMap;
import org.opendedup.sdfs.Config;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.io.WritableCacheBuffer;
import org.opendedup.sdfs.network.HashClient;
import org.opendedup.sdfs.network.HashClientPool;
import org.opendedup.util.HashFunctions;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HCServiceProxy {

	private static final long KBYTE = 1024L;
	private static Logger log = Logger.getLogger("sdfs");
	public static HashMap<String, HashClientPool> writeServers = new HashMap<String, HashClientPool>();
	public static HashMap<String, HashClientPool> readServers = new HashMap<String, HashClientPool>();
	public static HashMap<String, HashClientPool> writehashRoutes = new HashMap<String, HashClientPool>();
	public static HashMap<String, HashClientPool> readhashRoutes = new HashMap<String, HashClientPool>();
	private static LRUMap readBuffers = new LRUMap(Main.systemReadCacheSize);
	private static HashMap<String, byte[]> readingBuffers = new HashMap<String, byte[]>();
	// private static LRUMap existingHashes = new
	// LRUMap(Main.systemReadCacheSize);
	private static int writePoolPos = 0;
	private static ReentrantLock readlock = new ReentrantLock();

	// private static boolean initialized = false;

	public static long getChunksFetched() {
		return -1;
	}

	private static long dupsFound;

	private static HashClient getReadHashClient(String name) throws Exception {
		HashClient hc = (HashClient) readhashRoutes.get(name).borrowObject();
		return hc;
	}

	private static void returnObject(String name, HashClient hc)
			throws IOException {
		readhashRoutes.get(name).returnObject(hc);
	}

	private static HashClient getWriteHashClient(String name) throws Exception {
		HashClient hc = (HashClient) readhashRoutes.get(name).borrowObject();
		return hc;
	}

	public static boolean writeChunk(byte[] hash, byte[] aContents,
			int position, int len, boolean sendChunk) throws IOException {
		boolean doop = false;
		if (Main.chunkStoreLocal) {
			doop = HashChunkService.hashExists(hash);
			if (!doop && sendChunk) {
				try {
					doop = HashChunkService.writeChunk(hash, aContents, 0,
							Main.CHUNK_LENGTH, false);

				} catch (Exception e) {
					throw new IOException(e);
				}
			}
		} else {

			byte[] hashRoute = { hash[0] };
			String db = StringUtils.getHexString(hashRoute);
			HashClient hc = null;
			try {
				hc = getWriteHashClient(db);
				doop = hc.hashExists(hash);
				if (!doop && sendChunk) {
					try {
						hc.writeChunk(hash, aContents, 0, len);

					} catch (Exception e) {
						hc.close();
						hc.openConnection();
						hc.writeChunk(hash, aContents, 0, len);
					}
				}
			} catch (Exception e1) {
				if (hc != null)
					returnObject(db, hc);
				// TODO Auto-generated catch block
				log.log(Level.SEVERE, "Unable to write chunk " + hash, e1);
				throw new IOException("Unable to write chunk " + hash);
			} finally {
				if (hc != null)
					returnObject(db, hc);
			}
		}
		return doop;
	}

	public static boolean hashExists(byte[] hash) throws IOException {
		boolean exists = false;
		if (Main.chunkStoreLocal) {
			exists = HashChunkService.hashExists(hash);

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
				log.log(Level.SEVERE, "unable to execute find hash for "
						+ hashStr);
				throw new IOException(e1);
			}
			try {
				exists = hc.hashExists(hash);
				if (exists) {
					// existingHashes.put(hashStr, hashStr);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				throw new IOException(e);
			} finally {
				try {
					returnObject(db, hc);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					log
							.log(
									Level.SEVERE,
									"unable to return network thread object to pool",
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
			HashChunk hc = HashChunkService.fetchChunk(hash);
			return hc.getData();
		} else {
			String hashStr = StringUtils.getHexString(hash);
			boolean reading = false;
			ByteCache cache = null;
			try {
				cache = (ByteCache) readBuffers.get(hashStr);
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
				if (readlock.isLocked())
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
							log.info("Timeout waiting for read " + hashStr);
						readingBuffers.remove(hashStr);
						break;
					}
				}
			}
			try {
				cache = (ByteCache) readBuffers.get(hashStr);
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
				returnObject(db, hc);
				return data;
			} catch (Exception e) {
				log.log(Level.WARNING, "Unable to fetch buffer " + hashStr, e);
				throw new IOException("Unable to fetch buffer " + hashStr);
			} finally {
				if (readlock.isLocked())
					readlock.unlock();
			}
		}

	}

	public static long getChunksRead() {
		return -1;
	}

	public static long getChunksWritten() {
		return -1;
	}

	public static double getKBytesRead() {
		return -1;
	}

	public static double getKBytesWrite() {
		return -1;
	}

	public static long getDupsFound() {
		return dupsFound;
	}

}
