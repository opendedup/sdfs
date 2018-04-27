package org.opendedup.sdfs.filestore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.DedupFile;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;
import org.opendedup.sdfs.servers.HCServiceProxy;

/**
 * 
 * @author Sam Silverberg
 * 
 *         DedupFileStore is a static class used to store, return, and clone
 *         dedup file maps. All dedup files should be accessed through this
 *         class exclusively.
 * 
 */
public class DedupFileStore {

	private static boolean closing = false;
	// private static ReentrantLock clock = new ReentrantLock();
	private static OpenFileMonitor openFileMonitor = null;
	/*
	 * stores open files in an LRU map. Files will be evicted based on the
	 * maxOpenFiles parameter
	 */
	private static ConcurrentHashMap<String, SparseDedupFile> openFile = new ConcurrentHashMap<String, SparseDedupFile>();
	
	/*
	private static LoadingCache<ByteLongArrayWrapper, AtomicLong> keyLookup = CacheBuilder.newBuilder()
			.maximumSize(100).concurrencyLevel(64)
			.removalListener(new RemovalListener<ByteLongArrayWrapper, AtomicLong>() {
				public void onRemoval(RemovalNotification<ByteLongArrayWrapper, AtomicLong> removal) {
					ByteLongArrayWrapper bk = removal.getKey();
					try {
						if(!HCServiceProxy.claimKey(bk.getData(), bk.getVal(),removal.getValue().get())) {
							SDFSLogger.getLog().debug("Unable to insert " +" hash=" + StringUtils.getHexString(bk.getData()) + " lh=" + bk.getVal());
						}
					} catch (Exception e) {
						SDFSLogger.getLog().error("unable to add reference",e);
					}
					
				}
			}).build(new CacheLoader<ByteLongArrayWrapper, AtomicLong>() {
				public AtomicLong load(ByteLongArrayWrapper key) throws KeyNotFoundException {
					return new AtomicLong(0);
				}
			});
*/
	/*
	 * Spawns to open file monitor. The openFile monitor is used to evict open
	 * files from the openFile hashmap.
	 */
	static {
		if (Main.maxInactiveFileTime > 0 && !Main.blockDev) {
			openFileMonitor = new OpenFileMonitor(10000, Main.maxInactiveFileTime);
		} 

	}

	public static void init() {

	}
	
	//private static boolean gcRunning;
	static ReentrantReadWriteLock gcLock = new ReentrantReadWriteLock();
	public static void gcRunning(boolean running) {
		/*
		gcLock.writeLock().lock();
		try{
			
		if(running) {
			gcRunning = true;
			keyLookup.invalidateAll();
		}
		
		else
			gcRunning = false;
		}finally {
			gcLock.writeLock().unlock();
		}
		*/
		
	}

	/**
	 * 
	 * @param mf
	 *            the metadata dedup file to get the DedupFile for. If no dedup
	 *            file map exists, one is created.
	 * @return the dedup file map associated with the MetaDataDedupFile
	 * @throws IOException
	 */
	private static ReentrantLock getDFLock = new ReentrantLock();

	public static void updateDedupFile(MetaDataDedupFile mf) {
		getDFLock.lock();
		try {
			DedupFile df = openFile.get(mf.getDfGuid());
			if (df != null)
				df.setMetaDataDedupFile(mf);
		} finally {
			getDFLock.unlock();
		}
	}

	public static boolean addRef(byte[] entry, long val,int ct,String lookupfilter) throws IOException {
		if (val == 1 || val == 0)
			return true;
		
		try {
			if(!Main.refCount || Arrays.equals(entry, WritableCacheBuffer.bk))
				return true;
			else {
				gcLock.readLock().lock();
				try {
				//if(gcRunning)
					return HCServiceProxy.claimKey(entry, val,ct,lookupfilter);
				/*
				ByteLongArrayWrapper bl = new ByteLongArrayWrapper(entry,val);
				try {
					keyLookup.get(bl).incrementAndGet();
					return true;
				} catch (ExecutionException e) {
					SDFSLogger.getLog().error("unable to increment", e);;
					return false;
				}*/
				}finally {
					gcLock.readLock().unlock();
				}
			}

		} finally {
		}
	}
	
	public static boolean removeRef(byte[] entry, long val,int ct,String lookupfilter) throws IOException {
		if (val == 1|| val == 0)
			return true;
		
		try {
			if(!Main.refCount || Arrays.equals(entry, WritableCacheBuffer.bk))
				return true;
			else {
				gcLock.readLock().lock();
				try {
				//if(gcRunning)
					return HCServiceProxy.claimKey(entry, val,-1*ct,lookupfilter);
				//ByteLongArrayWrapper bl = new ByteLongArrayWrapper(entry,val);
				//try {
					//keyLookup.get(bl).decrementAndGet();
					//return true;
				//} catch (ExecutionException e) {
					//SDFSLogger.getLog().error("unable to increment", e);;
					//return false;
				//}
				}finally {
					gcLock.readLock().unlock();
				}
			}

		} finally {
		}
	}

	

	public static DedupFile getDedupFile(MetaDataDedupFile mf) throws IOException {
		getDFLock.lock();
		SparseDedupFile df = null;
		try {
			if (!closing) {
				df = openFile.get(mf.getDfGuid());
				if (df == null) {
					df = new SparseDedupFile(mf);
				}
				return df;
			} else {
				throw new IOException("DedupFileStore is closed");
			}
		} finally {
			getDFLock.unlock();
		}
	}

	public static DedupFile openDedupFile(MetaDataDedupFile mf) throws IOException {
		getDFLock.lock();
		SparseDedupFile df = null;
		try {
			if (!closing) {
				df = openFile.get(mf.getDfGuid());
				if (df == null) {
					df = new SparseDedupFile(mf);
					DedupFileStore.openFile.put(df.getGUID(), df);
				}
				return df;
			} else {
				throw new IOException("DedupFileStore is closed");
			}
		} finally {
			getDFLock.unlock();
		}
	}

	/**
	 * Adds a dedup file to the openFile hashmap.
	 * 
	 * @param df
	 *            the dedup file to add to the openfile hashmap
	 * @throws IOException
	 */
	public static void addOpenDedupFiles(SparseDedupFile df) throws IOException {
		if (!closing) {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("adding dedupfile " + df.getMetaFile().getPath());
			//SDFSLogger.getLog().info("adding " + df.getGUID() + "pth=" + df.getMetaFile().getPath());
			if (openFile.size() >= Main.maxOpenFiles) {
				SDFSLogger.getLog().warn("open files reached " + openFile.size());
				for(Entry<String, SparseDedupFile> en : openFile.entrySet()) {
					SDFSLogger.getLog().warn("path=" + en.getValue().mf.getPath() + " guid="+en.getKey());
				}
				throw new IOException(
						"maximum number of files reached [" + Main.maxOpenFiles + "]. Too many open files");
			}
			openFile.put(df.getGUID(), df);
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("dedupfile cache size is " + openFile.size());
		} else {
			throw new IOException("DedupFileStore is closed");
		}
	}
	public static SparseDedupFile get(String guid) {
		return openFile.get(guid);
	}

	/**
	 * Clones a dedupFile
	 * 
	 * @param oldmf
	 *            the file to clone
	 * @param newmf
	 *            the location of the, new, cloned dedup file map.
	 * @return the new cloned Dedup file map
	 * @throws IOException
	 */
	public static DedupFile cloneDedupFile(MetaDataDedupFile oldmf, MetaDataDedupFile newmf) throws IOException {
		if (!closing) {
			if (oldmf.getDfGuid() == null)
				return null;
			else {
				DedupFile df = openFile.get(oldmf.getDfGuid());
				if (df == null) {
					df = new SparseDedupFile(oldmf);
				}
				try {
					return df.snapshot(newmf, true);
				} catch (Exception e) {
					throw new IOException(e);
				}
			}

		} else {
			throw new IOException("DedupFileStore is closed");
		}
	}

	/**
	 * removes an open dedup file map from the openFile hashmap.
	 * 
	 * @param mf
	 */
	public static void removeOpenDedupFile(String guid) {
		//SDFSLogger.getLog().info("removing " + guid);
		if(guid != null) {
			openFile.remove(guid);
		}
		
	}

	/**
	 * Checks if a file is open
	 * 
	 * @param mf
	 *            the meta file to check if its open.
	 * @return true if open
	 */
	public static boolean fileOpen(MetaDataDedupFile mf) {
		try {
			return openFile.containsKey(mf.getDfGuid());
		} catch (NullPointerException e) {
			return false;
		}
	}

	/**
	 * 
	 * @return the open files at an array.
	 */
	public static DedupFile[] getArray() {
		DedupFile[] dfr = new DedupFile[openFile.size()];
		openFile.values().toArray(dfr);
		return dfr;
	}

	/**
	 * Closes the DedupFileStore, openFiles, and the openFileMonitor.
	 */
	public static void close() {
		closing = true;
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("Open Files = " + openFile.size());
		if (openFileMonitor != null)
			openFileMonitor.close();
		if (openFile.size() > 0) {
			Object[] dfs = getArray();
			SDFSLogger.getLog().info("closing openfiles of size " + dfs.length);
			for (int i = 0; i < dfs.length; i++) {
				DedupFile df = (DedupFile) dfs[i];
				if (df != null) {
					try {
						df.forceClose();
					} catch (IOException e) {
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug("unable to Close " + df.getMetaFile().getPath(), e);
					}
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("Closed " + df.getMetaFile().getPath());
				}
			}
		} 
		/*
		if(Main.refCount) {
			keyLookup.invalidateAll();
		}
		*/

	}

	/**
	 * Flushes the write buffers for all open files.
	 */
	public static void flushAllFiles() {
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("flushing write caches of size " + openFile.size());
		Object[] dfs = getArray();
		for (int i = 0; i < dfs.length; i++) {
			DedupFile df = (DedupFile) dfs[i];
			try {
				df.writeCache();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("DSE Full", e);
			}
		}
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("write caches flushed");
		/*
		if(Main.refCount) {
			keyLookup.invalidateAll();
		}
		*/
	}
}
