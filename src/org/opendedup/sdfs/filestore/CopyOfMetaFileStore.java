package org.opendedup.sdfs.filestore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.FileClosedException;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.OSValidator;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.CacheLoader;
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

	private static LoadingCache<String, MetaDataDedupFile> pathMap = CacheBuilder
			.newBuilder().maximumSize(5000).concurrencyLevel(Main.writeThreads)
			.removalListener(new RemovalListener<String, MetaDataDedupFile>() {
				@Override
				public void onRemoval(
						RemovalNotification<String, MetaDataDedupFile> arg0) {
					arg0.getValue().sync();

				}
			}).build(new CacheLoader<String, MetaDataDedupFile>() {
				public MetaDataDedupFile load(String key) throws IOException,
						FileClosedException {
					MetaDataDedupFile mf = MetaDataDedupFile.getFile(key);
					return mf;
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

	public static void rename(String src, String dst) throws IOException, ExecutionException {
		Lock l = getMFLock.writeLock();
		l.lock();
		try {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"removing [" + dst + "] and replacing with [" + src
								+ "]");
			MetaDataDedupFile mf = getMF(src);
			mf.renameTo(dst);
			pathMap.invalidate(src);
			pathMap.invalidate(dst);
			pathMap.put(dst, mf);
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

	/**
	 * 
	 * @param path
	 *            the path to the MetaDataDedupFile
	 * @return the MetaDataDedupFile
	 */
	private static ReentrantReadWriteLock getMFLock = new ReentrantReadWriteLock();

	public static MetaDataDedupFile getMF(File f) throws IOException {
		try {
		MetaDataDedupFile mf = null;
		Lock l = getMFLock.readLock();
		l.lock();
		try {
			mf = pathMap.get(f.getPath());
			return mf;
		} finally {
			l.unlock();
		}
		}catch(Exception e) {
			SDFSLogger.getLog().error("while getting file", e);
			throw new IOException(e);
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
		Lock l = getMFLock.readLock();
		l.lock();
		try {
			return MetaDataDedupFile.getFile(f.getPath());
		} finally {
			l.unlock();
		}
	}

	public static MetaDataDedupFile getMF(String filePath) throws IOException {
		return getMF(new File(filePath));
	}

	/**
	 * 
	 * @param parent
	 *            path for the parent
	 * @param child
	 *            the child file
	 * @return the MetaDataDedupFile associated with this path.
	 * @throws ExecutionException 
	 * @throws IOException 
	 */
	public static MetaDataDedupFile getMF(File parent, String child) throws IOException {
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
	 * @throws ExecutionException 
	 */
	public static MetaDataDedupFile snapshot(String origionalPath,
			String snapPath, boolean overwrite, SDFSEvent evt)
			throws IOException, ExecutionException {
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
	 * @throws ExecutionException 
	 */
	public static MetaDataDedupFile snapshot(String origionalPath,
			String snapPath, boolean overwrite, SDFSEvent evt,
			boolean propigateEvent) throws IOException, ExecutionException {
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
		Lock l = getMFLock.readLock();
		l.lock();
		try {
			pathMap.invalidateAll();
			return true;
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("unable to commit transaction", e);
		} finally {
			l.unlock();
		}
		return false;
	}

	/**
	 * Removes a file from the jdbm db
	 * 
	 * @param guid
	 *            the guid for the MetaDataDedupFile
	 * @throws IOException 
	 */

	public static boolean removeMetaFile(String path) throws IOException {
		return removeMetaFile(path, true);
	}

	public static boolean removeMetaFile(String path, boolean propigateEvent) throws IOException {

		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("deleting " + path);
		Lock l = getMFLock.writeLock();
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
						return Files.deleteIfExists(p);
					} else if (isDir) {
						File ps = new File(path);

						File[] files = ps.listFiles();

						for (int i = 0; i < files.length; i++) {
							boolean sd = removeMetaFile(files[i].getPath(),
									propigateEvent);
							if (!sd) {
								SDFSLogger.getLog().warn(
										"delete failed : unable to delete ["
												+ files[i] + "]");
								return sd;
							}
						}
						return Files.deleteIfExists(p);
					} else {
						mf = getMF(new File(path));
						pathMap.invalidate(mf.getPath());

						Main.volume.updateCurrentSize(-1 * mf.length(), true);
						try {
							Main.volume.addActualWriteBytes(
									-1
											* mf.getIOMonitor()
													.getActualBytesWritten(),
									true);
							Main.volume.addDuplicateBytes(-1
									* mf.getIOMonitor().getDuplicateBlocks(),
									true);
							Main.volume.addVirtualBytesWritten(-1
									* mf.getIOMonitor()
											.getVirtualBytesWritten(), true);
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
			} else
				return true;
		}catch(Exception e) {
				SDFSLogger.getLog().error("unable to delete", e);
				throw new IOException(e);
			}
		 finally {
			try{
			pathMap.invalidate(path);
			}catch(Exception e) {}
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