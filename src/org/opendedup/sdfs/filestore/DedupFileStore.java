package org.opendedup.sdfs.filestore;

import java.io.IOException;

import java.util.logging.Logger;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.DedupFile;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDedupFile;

import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

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
	private static Logger log = Logger.getLogger("sdfs");
	private static OpenFileMonitor openFileMonitor = null;
	/*
	 * stores open files in an LRU map. Files will be evicted based on the
	 * maxOpenFiles parameter
	 */
	private static ConcurrentLinkedHashMap<String, DedupFile> openFile = ConcurrentLinkedHashMap
			.create(
					ConcurrentLinkedHashMap.EvictionPolicy.LRU,
					Main.maxOpenFiles + 1,
					Main.writeThreads,
					new ConcurrentLinkedHashMap.EvictionListener<String, DedupFile>() {
						public void onEviction(String key, DedupFile df) {
							log.finer("removing " + df.getMetaFile().getPath()
									+ "from cache");
							df.close();
						}

					});
	/*
	 * Spawns to open file monitor. The openFile monitor is used to evict open
	 * files from the openFile hashmap.
	 */
	static {
		if (Main.maxInactiveFileTime > 0) {
			openFileMonitor = new OpenFileMonitor(60000,
					Main.maxInactiveFileTime);
		}
	}

	/**
	 * 
	 * @param mf
	 *            the metadata dedup file to get the DedupFile for. If no dedup
	 *            file map exists, one is created.
	 * @return the dedup file map associated with the MetaDataDedupFile
	 * @throws IOException
	 */
	public static synchronized DedupFile getDedupFile(MetaDataDedupFile mf)
			throws IOException {
		
		if (!closing) {
			log.finer("getting dedupfile for " + mf.getPath() + "and df " + mf.getDfGuid());
			DedupFile df = null;
			if (mf.getDfGuid() == null) {
				try {
					df = new SparseDedupFile(mf);

					log.finer("creating new dedup file for " + mf.getPath());
				} catch (Exception e) {

				}
			} else {
				df = (DedupFile) openFile.get(mf.getDfGuid());
				if (df == null) {

					df = new SparseDedupFile(mf);

				}
			}
			if (df == null) {
				throw new IOException("Can't find dedup file for "
						+ mf.getPath() + " requested df=" + mf.getDfGuid());
			}
			return df;
		} else {
			throw new IOException("DedupFileStore is closed");
		}
	}

	/**
	 * Adds a dedup file to the openFile hashmap.
	 * 
	 * @param df
	 *            the dedup file to add to the openfile hashmap
	 * @throws IOException
	 */
	public static void addOpenDedupFile(DedupFile df) throws IOException {
		if (!closing) {
			if (!openFile.containsKey(df.getGUID())) {
				log.finer("adding dedupfile");
				openFile.put(df.getGUID(), df);
				log.finer("dedupfile cache size is " + openFile.size());
			}
		} else {
			throw new IOException("DedupFileStore is closed");
		}
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
	public static DedupFile cloneDedupFile(MetaDataDedupFile oldmf,
			MetaDataDedupFile newmf) throws IOException {
		if (!closing) {
			DedupFile df = (SparseDedupFile) openFile.get(oldmf.getDfGuid());
			if (df == null) {
				df = new SparseDedupFile(oldmf);
			}

			return df.snapshot(newmf);

		} else {
			throw new IOException("DedupFileStore is closed");
		}
	}

	/**
	 * removes an open dedup file map from the openFile hashmap.
	 * 
	 * @param mf
	 */
	public static void removeOpenDedupFile(MetaDataDedupFile mf) {
		if (!closing) {
			openFile.remove(mf.getDfGuid());
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
		return openFile.containsKey(mf.getDfGuid());
	}

	/**
	 * 
	 * @return the open files at an array.
	 */
	public static DedupFile[] getArray() {
		DedupFile[] dfr = new DedupFile[openFile.size()];
		dfr = openFile.values().toArray(dfr);
		return dfr;
	}

	/**
	 * Closes the DedupFileStore, openFiles, and the openFileMonitor.
	 */
	public static void close() {
		closing = true;
		System.out.println("closing write caches of size " + openFile.size());
		if (openFileMonitor != null)
			openFileMonitor.close();
		Object[] dfs = getArray();
		for (int i = 0; i < dfs.length; i++) {
			DedupFile df = (DedupFile) dfs[i];
			df.close();
		}
	}

	/**
	 * Flushes the write buffers for all open files.
	 */
	public static void flushAllFiles() {
		log.finer("flushing write caches of size " + openFile.size());
		Object[] dfs = getArray();
		for (int i = 0; i < dfs.length; i++) {
			DedupFile df = (DedupFile) dfs[i];
			try {
				df.writeCache();
			} catch (IOException e) {

			}
		}
		log.finer("write caches flushed");
	}
}
