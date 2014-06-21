package org.opendedup.sdfs.filestore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.OSValidator;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

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
public class CopyOfMetaFileStore {
	
	private static ReentrantLock getMFLock = new ReentrantLock();

	private static LoadingCache<String, MetaDataDedupFile> pathMap = CacheBuilder
			.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).concurrencyLevel(Main.writeThreads).maximumSize(Main.maxOpenFiles).removalListener(new RemovalListener<String, MetaDataDedupFile>() {

				@Override
				public void onRemoval(
						RemovalNotification<String, MetaDataDedupFile> removal) {
					removal.getValue().unmarshal();
				}
			}).build(new CacheLoader<String, MetaDataDedupFile>() {

				@Override
				public MetaDataDedupFile load(String path) throws Exception {
					
					return MetaDataDedupFile.getFile(path);
				}
				
			});

	static {
		if (Main.version.startsWith("0.8")) {
			SDFSLogger.getLog().fatal(
					"Incompatible volume must be at least version 0.9.0 current volume vesion is ["
							+ Main.version + "]");
			System.exit(-1);
		}

	}

	public static void rename(String src, String dst, MetaDataDedupFile mf) {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"removing [" + dst + "] and replacing with [" + src
								+ "]");
			getMFLock.lock();
			try {
			pathMap.invalidate(src);
			pathMap.invalidate(dst);
			}finally {
				getMFLock.unlock();
			}
	}

	/**
	 * Removes a cached file from the pathmap
	 * 
	 * @param path
	 *            the path of the MetaDataDedupFile
	 */
	public static void removedCachedMF(String path) {
		getMFLock.lock();
		try{
		pathMap.invalidate(path);
		}finally {
			getMFLock.unlock();
		}
	}

	/**
	 * 
	 * @param path
	 *            the path to the MetaDataDedupFile
	 * @return the MetaDataDedupFile
	 * @throws ExecutionException 
	 */

	public static MetaDataDedupFile getMF(File f) throws ExecutionException {
		MetaDataDedupFile mf = null;
		getMFLock.lock();
		try{
		mf = pathMap.get(f.getPath());
		}finally {
			getMFLock.unlock();
		}
		

		return mf;

	}

	public static MetaDataDedupFile getFolder(File f) {
		
			return MetaDataDedupFile.getFile(f.getPath());
	}

	public static MetaDataDedupFile getMF(String filePath) throws ExecutionException {
		getMFLock.lock();
		try {
		return getMF(new File(filePath));
		}finally {
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
	 * @throws ExecutionException 
	 */
	public static MetaDataDedupFile getMF(File parent, String child) throws ExecutionException {
		String pth = parent.getAbsolutePath() + File.separator + child;
		return getMF(new File(pth));
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
			String snapPath, boolean overwrite, SDFSEvent evt)
			throws IOException {
		return snapshot(origionalPath, snapPath, overwrite, evt, true);
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
	 * @param propigateEvent
	 *            TODO
	 * @return the destination file.
	 * @throws IOException
	 */
	public static MetaDataDedupFile snapshot(String origionalPath,
			String snapPath, boolean overwrite, SDFSEvent evt,
			boolean propigateEvent) throws IOException {
		getMFLock.lock();
		try {
			Path p = Paths.get(origionalPath);
			if (Files.isSymbolicLink(p)) {

				MetaDataDedupFile mf = getMF(new File(origionalPath));
				File dst = new File(snapPath);
				File src = new File(mf.getPath());
				if (dst.exists() && !overwrite) {
					throw new IOException(snapPath + " already exists");
				}
				Path srcP = Paths.get(src.getPath());
				Path dstP = Paths.get(dst.getPath());
				try {
					Files.createSymbolicLink(dstP, srcP);
				} catch (IOException e) {
					SDFSLogger.getLog().error(
							"error symlinking " + origionalPath + " to "
									+ snapPath, e);
				}
				return mf;
			} else {

				MetaDataDedupFile mf = getMF(new File(origionalPath));
				if (mf == null)
					throw new IOException(
							origionalPath
									+ " does not exist. Cannot take a snapshot of a non-existent file.");
				synchronized (mf) {
					MetaDataDedupFile _mf = mf.snapshot(snapPath, overwrite,
							evt);
					return _mf;
				}
			}
		} catch (ExecutionException e1) {
			throw new IOException(e1);
		} finally {
			getMFLock.unlock();
		}
	}

	/**
	 * Commits data to the jdbm database
	 * 
	 * @return true if committed
	 */
	public static boolean commit() {
		getMFLock.lock();
		try {
			int z = (int)pathMap.size();
			pathMap.invalidateAll();
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("flushed " + z + " files ");
			// recman.commit();
			return true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to commit transaction", e);
		} finally {
			getMFLock.unlock();
		}
		return false;
	}

	public static boolean removeMetaFile(String path) {
		return removeMetaFile(path, true);
	}

	public static boolean removeMetaFile(String path, boolean propigateEvent) {
		getMFLock.lock();
		try {
			MetaDataDedupFile mf = null;
			boolean deleted = false;
			try {
				Path p = Paths.get(path);
				boolean isDir = false;
				boolean isSymlink = false;
				if (!OSValidator.isWindows()) {
					isDir = Files.readAttributes(p, PosixFileAttributes.class,
							LinkOption.NOFOLLOW_LINKS).isDirectory();
					isSymlink = Files.readAttributes(p,
							PosixFileAttributes.class,
							LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
				} else {
					isDir = new File(path).isDirectory();
				}
				if (isDir) {
					File ps = new File(path);

					File[] files = ps.listFiles();

					for (int i = 0; i < files.length; i++) {
						boolean sd = removeMetaFile(files[i].getPath(),
								propigateEvent);
						files[i].delete();
						if (!sd) {
							SDFSLogger.getLog().warn(
									"delete failed : unable to delete ["
											+ files[i] + "]");
							return sd;
						}
					}
					return (ps.delete());
				}
				if (isSymlink) {
					p.toFile().delete();
					p = null;
					return true;
				} else {
					mf = getMF(new File(path));
					pathMap.invalidate(path);

					Main.volume.updateCurrentSize(-1 * mf.length(), true);
					try {
						Main.volume.addActualWriteBytes(-1
								* mf.getIOMonitor().getActualBytesWritten(),
								true);
						Main.volume.addDuplicateBytes(-1
								* mf.getIOMonitor().getDuplicateBlocks(), true);
						Main.volume.addVirtualBytesWritten(-1
								* mf.getIOMonitor().getVirtualBytesWritten(),
								true);
					} catch (Exception e) {

					}
					if (mf.getDfGuid() != null) {
						try {
							deleted = mf.getDedupFile().delete(true);
						} catch (Exception e) {
							if (SDFSLogger.isDebug())
								SDFSLogger.getLog().debug(
										"unable to delete dedup file for "
												+ path, e);
						}
					}
					deleted = mf.deleteStub();
					if (!deleted) {
						SDFSLogger.getLog().warn(
								"could not delete " + mf.getPath());
						return deleted;
					}
				}
			} catch (Exception e) {
				if (mf != null) {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog()
								.debug("unable to remove " + path, e);
				}
				if (mf == null) {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug(
								"unable to remove  because [" + path
										+ "] is null");
				}
			}
			mf = null;
			return deleted;
		} finally {
			getMFLock.unlock();
		}
	}

	public static void close() {
		SDFSLogger.getLog().info("Closing metafilestore");
		try {
			commit();
		} catch (Exception e) {

		}
		SDFSLogger.getLog().info("metafilestore closed");
	}

}
