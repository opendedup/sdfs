package org.opendedup.sdfs.filestore;

import java.io.File;


import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.Attributes;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.btree.BTree;
import jdbm.helper.DefaultSerializer;
import jdbm.helper.StringComparator;
import jdbm.helper.compression.LeadingValueCompressionProvider;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer;

import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

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
	private static Logger log = Logger.getLogger("sdfs");
	private static ConcurrentLinkedHashMap<String, MetaDataDedupFile> pathMap = ConcurrentLinkedHashMap
	.create(
			ConcurrentLinkedHashMap.EvictionPolicy.LRU,
			10000,
			Main.writeThreads,
			new ConcurrentLinkedHashMap.EvictionListener<String, MetaDataDedupFile>() {
				// This method is called just after a new entry has been
				// added
				public void onEviction(String key,
						MetaDataDedupFile file) {
					file.unmarshal();
				}
			});

	


	/**
	 * caches a file to the pathmap
	 * 
	 * @param mf
	 */
	private static void cacheMF(MetaDataDedupFile mf) {
		pathMap.put(mf.getPath(), mf);
	}
	
	public static void rename(String src,String dst,MetaDataDedupFile mf){
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
	public static synchronized MetaDataDedupFile getMF(String path) {
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
			for (int i = 0; i <files.length; i++) {
				MetaDataDedupFile buf = (MetaDataDedupFile) files[i];
				buf.unmarshal();
				z++;
			}
			log.finer("flushed " + z + " files ");
			//recman.commit();
			return true;
		} catch (Exception e) {
			log.log(Level.SEVERE,"unable to commit transaction",e);
		}
		return false;
	}

	/**
	 * Removes a file from the jdbm db
	 * 
	 * @param guid
	 *            the guid for the MetaDataDedupFile
	 */
	public static synchronized boolean removeMetaFile(String path) {
		MetaDataDedupFile mf = null;
		boolean deleted = false;
		try {
			Path p = Paths.get(path);
			boolean isDir = Attributes.readBasicFileAttributes(p,
					LinkOption.NOFOLLOW_LINKS).isDirectory();
			boolean isSymlink = Attributes.readBasicFileAttributes(p,
					LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
			if(isDir) {
				File ps = new File(path);
				File[] files = ps.listFiles();
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						removeMetaFile(files[i].getPath());
					} else {
						files[i].delete();
					}
				}
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
					log.info("could not delete " + mf.getPath());
					return deleted;
				}
			}
		} catch (Exception e) {
			if (mf != null)
				log.log(Level.FINEST, "unable to remove " + path, e);
			if (mf == null)
				log.log(Level.FINEST, "unable to remove  because [" + path
						+ "] is null");
		}
		mf = null;
		return deleted;
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
