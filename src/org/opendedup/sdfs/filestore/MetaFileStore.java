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

import org.apache.commons.collections.map.LRUMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;

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
	private static RecordManager recman;
	// CacheRecordManager recman;
	private static BTree<String, MetaDataDedupFile> mftable;
	// A quick lookup table for path to MetaDataDedupFile
	private static transient LRUMap pathMap = new LRUMap(100);
	private static Logger log = Logger.getLogger("sdfs");

	static {
		Properties _props = new Properties();
		_props.put(RecordManagerOptions.CACHE_SIZE, "100");
		_props.put(RecordManagerOptions.AUTO_COMMIT, "false");
		// _props.put(RecordManagerOptions.COMPRESSOR,
		// RecordManagerOptions.COMPRESSOR_BEST_SPEED);
		// _props.put(RecordManagerOptions.DISABLE_TRANSACTIONS, "true");
		// _props.put(RecordManagerOptions.CACHE_TYPE ,
		// RecordManagerOptions.SOFT_REF_CACHE);
		/*
		 * Properties props = new Properties();
		 * props.put(RecordManagerOptions.CACHE_TYPE ,
		 * RecordManagerOptions.SOFT_REF_CACHE);
		 * props.put(RecordManagerOptions.AUTO_COMMIT, "true"); props.put(
		 * RecordManagerOptions.CACHE_SIZE, "2000000" );
		 */
		try {
			File f = new File(Main.metaDBStore);
			if (!f.exists())
				f.mkdirs();
			recman = RecordManagerFactory.createRecordManager(Main.metaDBStore
					+ File.separator + "mfstore", _props);
			long recid = recman.getNamedObject("metaFile");
			if (recid != 0) {

				mftable = BTree.load(recman, recid);
				log.fine("Entries " + mftable.entryCount());
				log.info("Reloaded existing meta file store");
			} else {
				mftable = BTree.createInstance(recman, new StringComparator(),
						new DefaultSerializer(), new DefaultSerializer());
				log.info("Total File Entries [" + mftable.entryCount() + "]");
				mftable
						.setKeyCompressionProvider(new LeadingValueCompressionProvider());
				recman.setNamedObject("metaFile", mftable.getRecid());
				log.fine("Created meta file store");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @return the number of entries within the jdbm database
	 */
	public static long getEntries() {
		return mftable.entryCount();
	}

	/**
	 * caches a file to the pathmap
	 * 
	 * @param mf
	 */
	private static void cacheMF(MetaDataDedupFile mf) {
		pathMap.put(mf.getPath(), mf);
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
			return new MetaDataDedupFile(f.getPath());
		}
		MetaDataDedupFile mf = (MetaDataDedupFile) pathMap.get(f.getPath());
		if (mf == null) {
			mf = new MetaDataDedupFile(f.getPath());
			setMetaFile(mf);
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
			boolean isSymbolicLink = Attributes.readBasicFileAttributes(p,
					LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
			boolean isDir = Attributes.readBasicFileAttributes(p,
					LinkOption.NOFOLLOW_LINKS).isDirectory();
			if (isSymbolicLink || isDir) {
				p.delete();
				p = null;
				return true;
			} else {
				mf = getMF(path);
				commit();
				pathMap.remove(mf.getPath());
				deleted = mf.getDedupFile().delete();
				if (!deleted)
					return deleted;
				else {
					Main.volume.updateCurrentSize(-1 * mf.length());
					deleted = mf.deleteStub();
					mftable.remove(mf.getGUID());
				}
			}
		} catch (Exception e) {
			if (mf != null)
				log.log(Level.FINEST, "unable to remove " + path, e);
			if (mf == null)
				log.log(Level.FINEST, "unable to remove  because [" + path
						+ "] is null");
		}
		log.finest(" meta-file size is " + mftable.entryCount());
		mf = null;
		return deleted;
	}

	/**
	 * Adds a MetaDataDedupFile to the jdbm database
	 * 
	 * @param mf
	 *            the MetaDataDedupFile
	 * @return true if committed
	 */
	public static boolean setMetaFile(MetaDataDedupFile mf) {
		try {
			mftable.insert(mf.getGUID(), mf, true);
			cacheMF(mf);
			return commit();
		} catch (IOException e) {
			log.log(Level.SEVERE, "unable to add " + mf.getGUID(), e);
			return false;
		}
	}

	/**
	 * Gets a file by the guid
	 * 
	 * @param guid
	 *            the guid of the MetaDataDedupFile
	 * @return the MetaDataDedupFile if it exists, otherwise it returns null.
	 */
	public static MetaDataDedupFile getMetaFile(String guid) {
		try {
			return mftable.find(guid);
		} catch (IOException e) {
			log.log(Level.SEVERE, "unable to get metafile for " + guid, e);
		}
		return null;
	}

	/**
	 * closes the jdbm database.
	 */
	public static void close() {
		System.out.println("Closing metafilestore");
		try {
			commit();
			recman.close();
		} catch (Exception e) {
		}
		System.out.println("metafilestore closed");
	}

}
