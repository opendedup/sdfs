package org.opendedup.sdfs.filestore;

import java.io.File;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.sdfs.io.events.MFileWritten;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.OSValidator;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.eventbus.EventBus;

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

	private static EventBus eventBus = new EventBus();

	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

	private static LoadingCache<String, MetaDataDedupFile> pathMap = CacheBuilder
			.newBuilder().concurrencyLevel(Main.writeThreads).maximumSize(1024)
			.removalListener(new RemovalListener<String, MetaDataDedupFile>() {
				// This method is called just after a new entry has been
				// added
				@Override
				public void onRemoval(
						RemovalNotification<String, MetaDataDedupFile> removal) {
					if (removal.getValue().exists())
						removal.getValue().sync();
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
			System.err
					.println("Incompatible volume must be at least version 0.9.0 current volume vesion is ["
							+ Main.version + "]");
			System.exit(-1);
		}
	}

	public static boolean rename(String src, String dst) throws IOException {
		WriteLock l =getMFLock.writeLock();
		l.lock();
		try {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"removing [" + dst + "] and replacing with [" + src
								+ "]");
			MetaDataDedupFile mf = getMF(src);
			boolean rn = mf.renameTo(dst);
			pathMap.invalidate(src);
			pathMap.invalidate(dst);
			pathMap.put(dst, mf);
			return rn;
		} finally {
			l.unlock();
		}
	}

	/**
	 * Removes a cached file from the pathmap
	 * 
	 * @param path
	 *            the path of the MetaDataDedupFile
	 */
	public static void removedCachedMF(String path) {
		pathMap.invalidate(path);
	}

	public static void addToCache(MetaDataDedupFile mf) {
		pathMap.put(mf.getPath(), mf);
	}

	/**
	 * 
	 * @param path
	 *            the path to the MetaDataDedupFile
	 * @return the MetaDataDedupFile
	 */
	private static ReentrantReadWriteLock getMFLock = new ReentrantReadWriteLock();

	public static MetaDataDedupFile getMF(File f) {
		ReadLock l = getMFLock.readLock();
		l.lock();
		try {
			return pathMap.get(f.getPath());
		} catch (Exception e) {
			SDFSLogger.getLog().debug("unable to get " + f.getPath(), e);
			return null;
		}finally {
			l.unlock();
		}
	}

	public static void mkDir(File f, int mode) throws IOException {
		if (f.exists()) {
			f = null;
			throw new IOException("folder exists");
		}
		f.mkdir();
		Path p = Paths.get(f.getPath());
		try {
			if (!OSValidator.isWindows())
				Files.setAttribute(p, "unix:mode", Integer.valueOf(mode));
		} catch (IOException e) {
			SDFSLogger.getLog().error("error while making dir " + f.getPath(),
					e);
			throw new IOException("access denied for " + f.getPath());
		}
	}

	public static MetaDataDedupFile getFolder(File f) {
		WriteLock l =getMFLock.writeLock();
		l.lock();
		try {
			return MetaDataDedupFile.getFile(f.getPath());
		} finally {
			l.unlock();
		}
	}

	public static MetaDataDedupFile getMF(String filePath) {
		return getMF(new File(filePath));
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
		try {
			Path p = Paths.get(origionalPath);
			if (Files.isSymbolicLink(p)) {
				File dst = new File(snapPath);
				File src = Files.readSymbolicLink(p).toFile();
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
				return getMF(new File(snapPath));
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
		} finally {

		}
	}

	/**
	 * Commits data to the jdbm database
	 * 
	 * @return true if committed
	 */
	public static boolean commit() {
		try {
			pathMap.invalidateAll();
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

	public static boolean removeMetaFile(String path) {
		return removeMetaFile(path, false);
	}

	public static boolean removeMetaFile(String path, boolean localOnly) {

		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("deleting " + path);
		WriteLock l =getMFLock.writeLock();
		l.lock();
		try {
			if (new File(path).exists()) {
				MetaDataDedupFile mf = null;
				boolean deleted = false;
				try {
					Path p = Paths.get(path);
					boolean isDir = false;
					boolean isSymlink = false;
					if (!OSValidator.isWindows()) {
						isDir = Files.readAttributes(p,
								PosixFileAttributes.class,
								LinkOption.NOFOLLOW_LINKS).isDirectory();
						isSymlink = Files.readAttributes(p,
								PosixFileAttributes.class,
								LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
					} else {
						isDir = new File(path).isDirectory();
					}
					if (isSymlink) {
						mf = getMF(new File(path));
						if (!localOnly)
							eventBus.post(new MFileDeleted(mf, true));
						deleted = Files.deleteIfExists(p);
						if (!localOnly && !deleted)
							eventBus.post(new MFileWritten(mf));
					} else if (isDir) {
						File ps = new File(path);

						File[] files = ps.listFiles();

						for (int i = 0; i < files.length; i++) {
							boolean sd = removeMetaFile(files[i].getPath(),
									localOnly);
							if (!sd) {
								SDFSLogger.getLog().warn(
										"delete failed : unable to delete ["
												+ files[i] + "]");
								return sd;
							}
						}
						mf = getMF(new File(path));
						if (!localOnly)
							eventBus.post(new MFileDeleted(mf, true));
						deleted = Files.deleteIfExists(p);
						if (!deleted && !localOnly)
							eventBus.post(new MFileWritten(mf));
					} else {
						mf = getMF(new File(path));
						
						deleted = mf.deleteStub();
						if (!deleted) {
							SDFSLogger.getLog().warn(
									"could not delete " + mf.getPath());
							return deleted;
						} else if (mf.getDfGuid() != null) {
							try {
								deleted = mf.getDedupFile(false).delete();
								pathMap.invalidate(mf.getPath());
							} catch (Exception e) {
								if (SDFSLogger.isDebug())
									SDFSLogger.getLog().debug(
											"unable to delete dedup file for "
													+ path, e);
							}
						}
						if (deleted) {
							Main.volume.updateCurrentSize(-1 * mf.length(),
									true);
							try {
								Main.volume.addActualWriteBytes(-1
										* mf.getIOMonitor()
												.getActualBytesWritten(), true);
								Main.volume.addDuplicateBytes(-1
										* mf.getIOMonitor()
												.getDuplicateBlocks(), true);
								Main.volume
										.addVirtualBytesWritten(
												-1
														* mf.getIOMonitor()
																.getVirtualBytesWritten(),
												true);
							} catch (Exception e) {

							}
						}
					}
				} catch (Exception e) {
					if (mf != null) {
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug(
									"unable to remove " + path, e);
					}
					if (mf == null) {
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug(
									"unable to remove  because [" + path
											+ "] is null", e);
					}
				}
				mf = null;
				return deleted;
			} 
				return true;
		} finally {
			l.unlock();
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