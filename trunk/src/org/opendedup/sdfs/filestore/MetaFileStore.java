package org.opendedup.sdfs.filestore;

import java.io.File;

import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.Attributes;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.util.SDFSLogger;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;

/**
 * 
 * @author Sam Silverberg
 * 
 *         The MetaFileStore is a static class used to get, update, create, or
 *         clone MetaDataDedup files. MetaDataDedupFile(s) are serialized to a
 *         JDBM database with a key of the uuid for the MetaDataDedupFile.
 * 
 * 
 */
public class MetaFileStore {

	// private static String dbURL =
	// "jdbc:derby:myDB;create=true;user=me;password=mine";

	// A quick lookup table for path to MetaDataDedupFile

	private static ConcurrentLinkedHashMap<String, MetaDataDedupFile> pathMap = new Builder<String, MetaDataDedupFile>()
			.concurrencyLevel(Main.writeThreads).maximumWeightedCapacity(10000)
			.listener(new EvictionListener<String, MetaDataDedupFile>() {
				// This method is called just after a new entry has been
				// added
				public void onEviction(String key, MetaDataDedupFile file) {
					file.unmarshal();
				}
			}).build();

	static {
		if (Main.version.startsWith("0.8")) {
			SDFSLogger.getLog().fatal(
					"Incompatible volume must be at least version 0.9.0 current volume vesion is ["
							+ Main.version + "]");
			System.exit(-1);
		}

	}

	/**
	 * caches a file to the pathmap
	 * 
	 * @param mf
	 */
	private static void cacheMF(MetaDataDedupFile mf) {
		pathMap.put(mf.getPath(), mf);
	}

	public static void rename(String src, String dst, MetaDataDedupFile mf) {
		pathMap.remove(src);
		pathMap.put(dst, mf);
	}

	/**
	 * Removes a cached file from the pathmap
	 * 
	 * @param path
	 *            the path of the MetaDataDedupFile
	 */
	public static void removedCachedMF(String path) {
		pathMap.remove(path);
	}

	/**
	 * 
	 * @param path
	 *            the path to the MetaDataDedupFile
	 * @return the MetaDataDedupFile
	 */
	private static ReentrantLock getMFLock = new ReentrantLock();

	public static MetaDataDedupFile getMF(String path) {
		getMFLock.lock();
		try {
			File f = new File(path);
			if (f.isDirectory()) {
				return MetaDataDedupFile.getFile(f.getPath());
			}
			MetaDataDedupFile mf = (MetaDataDedupFile) pathMap.get(f.getPath());
			if (mf == null) {
				mf = MetaDataDedupFile.getFile(f.getPath());
				cacheMF(mf);
			}
			return mf;
		} finally {
			getMFLock.unlock();
		}
	}

	/**
	 * 
	 * @param parent
	 *            path for the parent
	 * @param child
	 *            the child file
	 * @return the MetaDataDedupFile associated with this path.
	 */
	public static MetaDataDedupFile getMF(File parent, String child) {
		String pth = parent.getAbsolutePath() + File.separator + child;
		return getMF(pth);
	}

	/**
	 * Clones a MetaDataDedupFile and the DedupFile.
	 * 
	 * @param origionalPath
	 *            the path of the source
	 * @param snapPath
	 *            the path of the destination
	 * @param overwrite
	 *            whether or not to overwrite the destination if it exists
	 * @return the destination file.
	 * @throws IOException
	 */
	public static MetaDataDedupFile snapshot(String origionalPath,
			String snapPath, boolean overwrite) throws IOException {
		MetaDataDedupFile mf = getMF(origionalPath);
		synchronized (mf) {
			MetaDataDedupFile _mf = mf.snapshot(snapPath, overwrite);
			return _mf;
		}
	}

	/**
	 * Commits data to the jdbm database
	 * 
	 * @return true if committed
	 */
	public static boolean commit() {
		try {
			Object[] files = pathMap.values().toArray();
			int z = 0;
			for (int i = 0; i < files.length; i++) {
				MetaDataDedupFile buf = (MetaDataDedupFile) files[i];
				buf.unmarshal();
				z++;
			}
			SDFSLogger.getLog().debug("flushed " + z + " files ");
			// recman.commit();
			return true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to commit transaction", e);
		}
		return false;
	}

	/**
	 * Removes a file from the jdbm db
	 * 
	 * @param guid
	 *            the guid for the MetaDataDedupFile
	 */
	private static ReentrantLock removeMFLock = new ReentrantLock();

	public static boolean removeMetaFile(String path) {
		removeMFLock.lock();
		try {
			MetaDataDedupFile mf = null;
			boolean deleted = false;
			try {
				Path p = Paths.get(path);
				boolean isDir = Attributes.readBasicFileAttributes(p,
						LinkOption.NOFOLLOW_LINKS).isDirectory();
				boolean isSymlink = Attributes.readBasicFileAttributes(p,
						LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
				if (isDir) {
					File ps = new File(path);
					/*
					 * File[] files = ps.listFiles();
					 * 
					 * for (int i = 0; i < files.length; i++) { if
					 * (files[i].isDirectory()) {
					 * removeMetaFile(files[i].getPath()); } else {
					 * files[i].delete(); } }
					 */
					return (ps.delete());
				}
				if (isSymlink) {
					p.delete();
					p = null;
					return true;
				} else {
					mf = getMF(path);
					pathMap.remove(mf.getPath());
					deleted = mf.getDedupFile().delete();
					deleted = mf.deleteStub();
					Main.volume.updateCurrentSize(-1 * mf.length());
					if (!deleted) {
						SDFSLogger.getLog().info(
								"could not delete " + mf.getPath());
						return deleted;
					}
				}
			} catch (Exception e) {
				if (mf != null)
					SDFSLogger.getLog().debug("unable to remove " + path, e);
				if (mf == null)
					SDFSLogger.getLog().debug(
							"unable to remove  because [" + path + "] is null");
			}
			mf = null;
			return deleted;
		} finally {
			removeMFLock.unlock();
		}
	}

	/**
	 * closes the jdbm database.
	 */
	public static void close() {
		System.out.println("Closing metafilestore");
		try {
			commit();
		} catch (Exception e) {
		}
		System.out.println("metafilestore closed");
	}

}