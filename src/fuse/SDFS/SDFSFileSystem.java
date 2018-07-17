package fuse.SDFS;

import java.io.File;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.sdfs.io.events.MFileWritten;
import org.opendedup.util.StringUtils;

import com.google.common.eventbus.EventBus;
import com.google.common.io.BaseEncoding;

import fuse.Errno;
import fuse.Filesystem3;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseFtypeConstants;
import fuse.FuseGetattrSetter;
import fuse.FuseOpenSetter;
import fuse.FuseSizeSetter;
import fuse.FuseStatfsSetter;
import fuse.XattrLister;
import fuse.XattrSupport;

public class SDFSFileSystem implements Filesystem3, XattrSupport {

	public String mountedVolume;
	public String mountPoint;
	public String connicalMountedVolume;
	public static long MAXHDL = Long.MAX_VALUE - 10000;
	private static final int BLOCK_SIZE = 32768;
	private static final int NAME_LENGTH = 2048;
	static long tbc = 1099511627776L;
	static int gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;
	static int kbc = 1024;
	private SDFSCmds sdfsCmds;
	private static EventBus eventBus = new EventBus();
	ConcurrentHashMap<Long, DedupFileChannel> dedupChannels = new ConcurrentHashMap<Long, DedupFileChannel>();
	long handleGen = 0;

	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

	private void checkInFS(File f) throws FuseException {
		try {

			
			if (!f.getCanonicalPath().startsWith(connicalMountedVolume)) {
				SDFSLogger.getLog()
						.warn("Path is not in mounted [" + mountedVolume + "]folder " + f.getCanonicalPath());
				throw new FuseException("data not in path " + f.getCanonicalPath()).initErrno(Errno.EACCES);
			}
		} catch (IOException e) {
			SDFSLogger.getLog().warn("Path is not in mounted folder", e);
			throw new FuseException("data not in path " + f.getPath()).initErrno(Errno.EACCES);
		}
	}

	public SDFSFileSystem(String mountedVolume, String mountPoint) throws IOException {

		SDFSLogger.getLog().info("mounting " + mountedVolume + " to " + mountPoint);
		this.mountedVolume = mountedVolume;
		if (!this.mountedVolume.endsWith("/"))
			this.mountedVolume = this.mountedVolume + "/";
		this.connicalMountedVolume = new File(this.mountedVolume).getCanonicalPath();
		this.mountPoint = mountPoint;
		if (!this.mountPoint.endsWith("/"))
			this.mountPoint = this.mountPoint + "/";
		sdfsCmds = new SDFSCmds(this.mountedVolume, this.mountPoint);
		File f = new File(this.mountedVolume);
		if (!f.exists())
			f.mkdirs();
	}

	@Override
	public int chmod(String path, int mode) throws FuseException {
		// SDFSLogger.getLog().info("12");
		try {
			File f = resolvePath(path);
			int ftype = this.getFtype(path);
			if (ftype == FuseFtypeConstants.TYPE_SYMLINK || ftype == FuseFtypeConstants.TYPE_DIR) {
				Path p = Paths.get(f.getCanonicalPath());

				try {
					Files.setAttribute(p, "unix:mode", Integer.valueOf(mode), LinkOption.NOFOLLOW_LINKS);
				} catch (IOException e) {
					SDFSLogger.getLog().warn("access denied for " + path, e);
					throw new FuseException("access denied for " + path).initErrno(Errno.EACCES);
				} finally {
					path = null;
				}
			} else {

				try {
					MetaDataDedupFile mf = MetaFileStore.getMF(f);
					mf.setMode(mode);
				} catch (Exception e) {
					SDFSLogger.getLog().warn("access denied for " + path, e);
					throw new FuseException("access denied for " + path).initErrno(Errno.EACCES);
				} finally {
				}
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error(path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
		return 0;
	}

	@Override
	public int chown(String path, int uid, int gid) throws FuseException {
		// SDFSLogger.getLog().info("17");
		try {
			File f = resolvePath(path);
			int ftype = this.getFtype(path);
			if (ftype == FuseFtypeConstants.TYPE_SYMLINK || ftype == FuseFtypeConstants.TYPE_DIR) {
				Path p = Paths.get(f.getCanonicalPath());
				try {
					Files.setAttribute(p, "unix:uid", Integer.valueOf(uid), LinkOption.NOFOLLOW_LINKS);
					Files.setAttribute(p, "unix:gid", Integer.valueOf(gid), LinkOption.NOFOLLOW_LINKS);
				} catch (IOException e) {
					e.printStackTrace();
					throw new FuseException("access denied for " + path).initErrno(Errno.EACCES);
				} finally {
					path = null;
				}
			} else {

				try {
					MetaDataDedupFile mf = MetaFileStore.getMF(f);
					mf.setOwner_id(uid);
					mf.setGroup_id(gid);
				} catch (Exception e) {
					SDFSLogger.getLog().warn("access denied for " + path, e);
					throw new FuseException("access denied for " + path).initErrno(Errno.EACCES);
				} finally {

				}
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error(path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
		return 0;
	}

	private DedupFileChannel getFileChannel(String path, long handleNo, int flags) throws FuseException {
		DedupFileChannel ch = this.dedupChannels.get(handleNo);
		if (ch == null) {
			File f = this.resolvePath(path);
			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
				ch = mf.getDedupFile(false).getChannel(flags);
				try {
					if (this.dedupChannels.containsKey(handleNo)) {
						ch.getDedupFile().unRegisterChannel(ch, flags);
						ch = this.dedupChannels.get(handleNo);
					} else {
						this.dedupChannels.put(handleNo, ch);
					}
				} catch (Exception e) {

				} finally {
					SDFSLogger.getLog().debug("number of channels is " + this.dedupChannels.size());
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to open file" + f.getPath(), e);
				throw new FuseException("unable to open file " + path).initErrno(Errno.EINVAL);
			}
		}
		return ch;
	}

	@Override
	public int flush(String path, long fh) throws FuseException {
		// SDFSLogger.getLog().info("109");
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		DedupFileChannel ch = this.getFileChannel(path, (Long) fh, -1);
		try {
			ch.force(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to sync file [" + path + "]", e);
			throw new FuseException("unable to sync file").initErrno(Errno.EACCES);
		} finally {
		}
		return 0;
	}

	@Override
	public int fsync(String path, long fh, boolean isDatasync) throws FuseException {
		// SDFSLogger.getLog().info("1000");
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		try {
			DedupFileChannel ch = this.getFileChannel(path, (Long) fh, -1);

			if (Main.safeSync) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("sync " + path + " df=" + ch.getDedupFile().getGUID());
				ch.force(true);
			}

		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to sync file [" + path + "]", e);
			throw new FuseException("unable to sync file").initErrno(Errno.EACCES);
		} finally {

		}
		return 0;
	}

	@Override
	public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
		SDFSLogger.getLog().info("0 " + path);

		try {
			int ftype = this.getFtype(path);
			//SDFSLogger.getLog().info("1 " + path + " " + ftype);
			if (ftype == FuseFtypeConstants.TYPE_SYMLINK) {
				// SDFSLogger.getLog().info("poop " + path);
				Path p = null;
				BasicFileAttributes attrs = null;
				try {
					p = Paths.get(this.mountedVolume + path);
					int uid = 0;
					int gid = 0;
					int mode = 0000;
					try {
						uid = (Integer) Files.getAttribute(p, "unix:uid", LinkOption.NOFOLLOW_LINKS);
						gid = (Integer) Files.getAttribute(p, "unix:gid", LinkOption.NOFOLLOW_LINKS);
						mode = (Integer) Files.getAttribute(p, "unix:mode", LinkOption.NOFOLLOW_LINKS);
					} catch (Exception e) {
						SDFSLogger.getLog().error("unable to parse sylink " + path, e);
					}

					int atime = 0;
					int ctime = 0;
					int mtime = 0;
					long fileLength = 0;
					attrs = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
					fileLength = attrs.size();
					mtime = (int) (attrs.lastModifiedTime().toMillis() / 1000L);
					atime = (int) (attrs.lastAccessTime().toMillis() / 1000L);
					ctime = (int) (attrs.creationTime().toMillis() / 1000L);

					getattrSetter.set(p.hashCode(), mode, 1, uid, gid, 0, fileLength,
							(fileLength * NAME_LENGTH + BLOCK_SIZE - 1) / BLOCK_SIZE, atime, mtime, ctime);
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to parse sylink " + path, e);
					throw new FuseException().initErrno(Errno.EACCES);
				} finally {
					attrs = null;
					p = null;
				}
			} else {
				File f = resolvePath(path);
				Path p = null;
				try {

					p = Paths.get(f.getCanonicalPath());
					if (ftype == FuseFtypeConstants.TYPE_DIR) {
						int uid = (Integer) Files.getAttribute(p, "unix:uid");
						int gid = (Integer) Files.getAttribute(p, "unix:gid");
						int mode = (Integer) Files.getAttribute(p, "unix:mode");
						MetaDataDedupFile mf = MetaFileStore.getFolder(f);
						int atime = (int) (mf.getLastAccessed() / 1000L);
						int mtime = (int) (mf.lastModified() / 1000L);
						int ctime = (int) (0 / 1000L);

						long fileLength = f.length();
						getattrSetter.set(mf.getGUID().hashCode(), mode, 1, uid, gid, 0, fileLength * NAME_LENGTH,
								(fileLength * NAME_LENGTH + BLOCK_SIZE - 1) / BLOCK_SIZE, atime, mtime, ctime);
					} else {
						MetaDataDedupFile mf = MetaFileStore.getMF(f);
						int uid = mf.getOwner_id();
						int gid = mf.getGroup_id();
						int mode = mf.getMode();
						int atime = (int) (mf.getLastAccessed() / 1000L);
						int ctime = (int) (0 / 1000L);
						int mtime = (int) (mf.lastModified() / 1000L);
						long fileLength = mf.length();
						// SDFSLogger.getLog().info("fileLength=" + fileLength + " path=" + path);
						long actualBytes = (mf.getIOMonitor().getActualBytesWritten() * 2) / 1024;
						if (actualBytes == 0 && mf.getIOMonitor().getActualBytesWritten() > 0)
							actualBytes = (Main.CHUNK_LENGTH * 2) / 1024;
						getattrSetter.set(mf.getGUID().hashCode(), mode, 1, uid, gid, 0, fileLength, actualBytes, atime,
								mtime, ctime);
					}
				} catch (Exception e) {
					SDFSLogger.getLog().error(
							"unable to parse attributes " + path + " at physical path " + f.getCanonicalPath(), e);
					throw new FuseException().initErrno(Errno.EACCES);
				} finally {
					f = null;
					p = null;
				}
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().warn(path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
		return 0;
	}

	@Override
	public int getdir(String path, FuseDirFiller dirFiller) throws FuseException {
		// SDFSLogger.getLog().info("1");
		try {
			File f = null;
			try {
				f = resolvePath(path);
				File[] mfs = f.listFiles();
				dirFiller.add(".", ".".hashCode(), FuseFtypeConstants.TYPE_DIR);
				dirFiller.add("..", "..".hashCode(), FuseFtypeConstants.TYPE_DIR);
				for (int i = 0; i < mfs.length; i++) {
					File _mf = mfs[i];
					// SDFSLogger.getLog().info("lf=" + _mf.getCanonicalPath());
					dirFiller.add(_mf.getName(), _mf.hashCode(), this.getFtype(_mf));
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to read path " + path, e);
				throw new FuseException().initErrno(Errno.EACCES);

			} finally {
				f = null;
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error(path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
		return 0;
	}

	@Override
	public int link(String from, String to) throws FuseException {
		// SDFSLogger.getLog().info("2");
		throw new FuseException("error hard linking is not supported").initErrno(Errno.ENOSYS);

		/*
		 * log.debug("link(): " + from + " to " + to); File dst = new File(mountedVolume
		 * + to); if (dst.exists()) { throw new FuseException("file exists")
		 * .initErrno(FuseException.EPERM); } Path srcP = Paths.get(from); Path dstP =
		 * Paths.get(dst.getCanonicalPath()); try { dstP.createLink(srcP); } catch
		 * (IOException e) { log.error("error linking " + from + " to " + to, e); throw
		 * new FuseException("error linking " + from + " to " + to)
		 * .initErrno(FuseException.EACCES); } finally { srcP = null; dstP = null; }
		 * return 0;
		 */
	}

	@Override
	public int mkdir(String path, int mode) throws FuseException {

		try {
			File f = new File(this.mountedVolume + path);
			try {
				this.checkInFS(f);
			} catch (FuseException e) {
				SDFSLogger.getLog().warn("unable", e);
				throw e;
			}
			// SDFSLogger.getLog().info("3 " + f.getCanonicalPath());
			if (Main.volume.isOffLine())
				throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
			if (Main.volume.isFull())
				throw new FuseException("Volume Full").initErrno(Errno.ENOSPC);
			if (f.exists()) {
				f = null;
				throw new FuseException("folder exists").initErrno(Errno.EPERM);
			}
			try {
				MetaFileStore.mkDir(f, mode);
				eventBus.post(new MFileWritten(MetaFileStore.getMF(f), true));
			} catch (IOException e) {
				SDFSLogger.getLog().error("error while making dir " + path, e);
				throw new FuseException("access denied for " + path).initErrno(Errno.EACCES);
			} finally {
				path = null;
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error(path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
		return 0;
	}

	@Override
	public int mknod(String path, int mode, int rdev) throws FuseException {
		// SDFSLogger.getLog().info("4=" + path);
		try {
			File f = new File(this.mountedVolume + path);
			try {
				this.checkInFS(f);
			} catch (FuseException e) {
				SDFSLogger.getLog().warn("unable", e);
				throw e;
			}
			/*
			 * if(!f.getCanonicalPath().startsWith(this.mountedVolume)) { f = null; throw
			 * new FuseException("file exists").initErrno(Errno.ENOENT); }
			 */
			if (Main.volume.isOffLine())
				throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
			if (Main.volume.isFull()) {
				// SDFSLogger.getLog().info("41=");
				throw new FuseException("Volume Full").initErrno(Errno.ENOSPC);
			}

			if (f.exists()) {
				// SDFSLogger.getLog().info("42=");
				f = null;
				throw new FuseException("file exists").initErrno(Errno.EEXIST);
			} else {
				// SDFSLogger.getLog().info("43=");
				MetaDataDedupFile mf = MetaFileStore.getMF(f);
				mf.unmarshal();
				// SDFSLogger.getLog().info("44=");
				/*
				 * // Wait up to 5 seconds for file to be created int z = 5000; int i = 0; while
				 * (!f.exists()) { i++; if (i == z) { throw new
				 * FuseException("file creation timed out for " + path).initErrno(Errno.EBUSY);
				 * } else { try { Thread.sleep(1); } catch (InterruptedException e) { throw new
				 * FuseException( "file creation interrupted for " + path)
				 * .initErrno(Errno.EACCES); } }
				 * 
				 * }
				 */
				try {
					mf.setMode(mode);
				} finally {
					f = null;
				}

			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error making " + path, e);
			throw new FuseException("access denied for " + path).initErrno(Errno.EACCES);
		}
		return 0;
	}

	ReentrantLock ol = new ReentrantLock();

	@Override
	public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
		// SDFSLogger.getLog().info("555=" + path);
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		try {
			long z = 0;
			ol.lock();
			try {
				handleGen++;
				z = handleGen;
				if (handleGen > MAXHDL)
					handleGen = 0;
			} finally {
				ol.unlock();
			}
			this.getFileChannel(path, z, flags);
			openSetter.setFh(z);
			// SDFSLogger.getLog().info("555=" + path + " z=" + z);
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while opening file", e);
			throw new FuseException("error opending " + path).initErrno(Errno.ENODATA);
		} finally {
		}
		return 0;
	}

	@Override
	public int read(String path, long fh, ByteBuffer buf, long offset) throws FuseException {
		// SDFSLogger.getLog().info("1911 " + path);
		if (Main.volume.isOffLine())
			throw new FuseException("Volume Offline").initErrno(Errno.ENODEV);
		try {
			DedupFileChannel ch = this.getFileChannel(path, (Long) fh, -1);
			int read = ch.read(buf, 0, buf.capacity(), offset);
			/*
			 * if (buf.position() < buf.capacity()) { byte[] k = new byte[buf.capacity() -
			 * buf.position()]; buf.put(k); // SDFSLogger.getLog().info("zzz=" //
			 * +(buf.capacity()-buf.position())); } byte[] b = new byte[buf.capacity()];
			 * buf.position(0); buf.get(b); buf.position(0);
			 * 
			 * SDFSLogger.getLog().info("read " + path + " len" + buf.capacity() + "offset "
			 * + offset + "read" + read + "==" + new String(b) + "==\n\n");
			 */
			if (read == -1)
				read = 0;

		} catch (DataArchivedException e) {
			throw new FuseException("File Archived").initErrno(Errno.ENODATA);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to read file " + path, e);
			throw new FuseException("error reading " + path).initErrno(Errno.ENODATA);
		}
		return 0;
	}

	@Override
	public int readlink(String path, CharBuffer link) throws FuseException {
		// SDFSLogger.getLog().info("190");
		Path p = Paths.get(this.mountedVolume + path);
		try {

			String lpath = Files.readSymbolicLink(p).toString();
			if (new File(lpath).getPath().startsWith(this.mountedVolume))
				lpath = this.mountPoint + lpath.substring(this.mountedVolume.length());
			// SDFSLogger.getLog().info("path=" + path + " lpath=" + lpath);
			link.put(lpath);
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting linking " + path, e);
			throw new FuseException("error getting linking " + path).initErrno(Errno.EACCES);
		} finally {
			p = null;
		}
		return 0;
	}

	@Override
	public int release(String path, long fh, int flags) throws FuseException {
		// SDFSLogger.getLog().info("199 " + path);
		try {
			DedupFileChannel ch = this.dedupChannels.remove((Long) fh);
			if (!Main.safeClose)
				return 0;
			ch.getDedupFile().unRegisterChannel(ch, flags);

			ch = null;

		} catch (Exception e) {
			SDFSLogger.getLog().error("error releasing" + path, e);
			throw new FuseException("error releasing " + path).initErrno(Errno.EBADFD);
		}
		return 0;
	}

	@Override
	public int rename(String from, String to) throws FuseException {
		// SDFSLogger.getLog().info("198 " + from + " " + to);
		File f = null;

		try {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("renaming [" + from + "] to [" + to + "]");
			f = resolvePath(from);
			File nf = new File(this.mountedVolume + to);
			try {
				this.checkInFS(nf);
			} catch (FuseException e) {
				SDFSLogger.getLog().warn("unable", e);
				throw e;
			}
			MetaFileStore.rename(f.getCanonicalPath(), nf.getCanonicalPath());
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to rename " + from + " to " + to, e);
			throw new FuseException().initErrno(Errno.EACCES);
		} finally {
			f = null;
		}
		return 0;
	}

	@Override
	public int rmdir(String path) throws FuseException {
		// SDFSLogger.getLog().info("197 " + path);
		try {
			if (this.getFtype(path) == FuseFtypeConstants.TYPE_SYMLINK) {

				File f = new File(mountedVolume + path);

				// SDFSLogger.getLog().info("deleting symlink " + f.getCanonicalPath());
				if (!f.delete()) {
					f = null;
					throw new FuseException().initErrno(Errno.EACCES);
				}
				return 0;
			} else {
				File f = resolvePath(path);
				if (f.getName().equals(".") || f.getName().equals(".."))
					return 0;
				else {
					try {

						if (MetaFileStore.removeMetaFile(f.getCanonicalPath(), false, false, true))
							return 0;
						else {

							if (SDFSLogger.isDebug())
								SDFSLogger.getLog().debug("unable to delete folder " + f.getCanonicalPath());
							throw new FuseException().initErrno(Errno.ENOTEMPTY);
						}

					} catch (FuseException e) {
						throw e;
					} catch (Exception e) {
						SDFSLogger.getLog().debug("unable to delete folder " + f.getCanonicalPath());
						throw new FuseException().initErrno(Errno.EACCES);
					}

				}
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error(path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
	}

	@Override
	public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
		// SDFSLogger.getLog().info("196");
		try {
			long blocks = Main.volume.getTotalBlocks();
			long used = Main.volume.getUsedBlocks();
			if (used > blocks)
				used = blocks;
			statfsSetter.set(Main.volume.getBlockSize(), blocks, blocks - used, blocks - used, 0, 0, NAME_LENGTH);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to stat", e);
			throw new FuseException().initErrno(Errno.EACCES);
		} finally {

		}
		return 0;
	}

	@Override
	public int symlink(String from, String to) throws FuseException {
		// SDFSLogger.getLog().info("195");
		try {
			File src = null;
			File fr = new File(mountedVolume + from);
			if (fr.getCanonicalPath().startsWith(this.mountPoint)) {
				from = from.substring(mountPoint.length());
				this.resolvePath(from);
				src = new File(mountedVolume + from);
			} else if (!Main.allowExternalSymlinks) {
				SDFSLogger.getLog().error("external symlinks are not allowed " + from + " to " + to);
				throw new FuseException().initErrno(Errno.EACCES);
			} else {
				src = new File(from);
			}
			File dst = new File(mountedVolume + to);
			try {
				this.checkInFS(dst);
			} catch (FuseException e) {
				SDFSLogger.getLog().warn("unable", e);
				throw e;
			}
			if (dst.exists()) {
				throw new FuseException().initErrno(Errno.EPERM);
			}
			Path srcP = Paths.get(src.getCanonicalPath());
			Path dstP = Paths.get(dst.getCanonicalPath());
			// SDFSLogger.getLog().info(
			// "symlink " + src.getCanonicalPath() + " to " + dst.getCanonicalPath());
			try {
				Files.createSymbolicLink(dstP, srcP);
				eventBus.post(new MFileWritten(MetaFileStore.getMF(dst.getCanonicalPath()), true));
			} catch (IOException e) {

				SDFSLogger.getLog().error("error linking " + from + " to " + to, e);
				throw new FuseException().initErrno(Errno.EACCES);
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error(from, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
		return 0;
	}

	@Override
	public int truncate(String path, long size) throws FuseException {
		// SDFSLogger.getLog().info("193 " + path + " size=" + size);
		try {
			DedupFileChannel ch = this.getFileChannel(path, -1);
			ch.truncateFile(size);
			ch.getDedupFile().unRegisterChannel(ch, -1);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to truncate file " + path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		} finally {
		}
		return 0;
	}

	@Override
	public int unlink(String path) throws FuseException {
		// SDFSLogger.getLog().info("22222 " + path);
		// Thread.currentThread().setName("19
		// "+Long.toString(System.currentTimeMillis()));
		try {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("removing " + path);

			if (!Main.safeClose) {
				try {
					this.getFileChannel(path, -1).getDedupFile().forceClose();
				} catch (IOException e) {
					SDFSLogger.getLog().error("unable to close file " + path, e);
				}
			}
			if (this.getFtype(path) == FuseFtypeConstants.TYPE_SYMLINK) {
				Path p = new File(mountedVolume + path).toPath();
				try {
					this.checkInFS(new File(mountedVolume + path));
				} catch (FuseException e) {
					SDFSLogger.getLog().warn("unable", e);
					throw e;
				}
				// SDFSLogger.getLog().info("deleting symlink " + f.getCanonicalPath());
				try {
					MetaDataDedupFile mf = MetaFileStore.getMF(this.resolvePath(path));
					eventBus.post(new MFileDeleted(mf));
					Files.delete(p);
					return 0;
				} catch (IOException e) {
					SDFSLogger.getLog().warn("unable to delete symlink " + p);
					throw new FuseException().initErrno(Errno.ENOSYS);
				}
			} else {

				File f = this.resolvePath(path);
				try {
					if (MetaFileStore.removeMetaFile(f.getPath())) {
						// SDFSLogger.getLog().info("deleted file " +
						// f.getCanonicalPath());
						return 0;
					} else if (MetaFileStore.removeMetaFile(f.getCanonicalPath())) {
						// SDFSLogger.getLog().info("deleted file " +
						// f.getCanonicalPath());
						return 0;
					} else {
						SDFSLogger.getLog().warn("unable to delete file " + f.getCanonicalPath());
						throw new FuseException().initErrno(Errno.EACCES);
					}
				} catch (FuseException e) {
					throw e;
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to file file " + path, e);
					throw new FuseException().initErrno(Errno.EACCES);
				}
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error(path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
	}

	@Override
	public int utime(String path, int atime, int mtime) throws FuseException {
		// SDFSLogger.getLog().info("19 " + path);
		// Thread.currentThread().setName("20
		// "+Long.toString(System.currentTimeMillis()));
		try {
			File f = this.resolvePath(path);
			if (f.isFile()) {
				MetaDataDedupFile mf = MetaFileStore.getMF(f);
				mf.setLastAccessed(atime * 1000L);
				mf.setLastModified(mtime * 1000L);
			} else {
				Path p = f.toPath();
				Files.setLastModifiedTime(p, FileTime.fromMillis(mtime * 1000L));
				MetaDataDedupFile mf = MetaFileStore.getMF(f);
				if (mf.isFile())
					mf.setDirty(true);
				eventBus.post(new MFileWritten(mf, true));
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable change utime path " + path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		} finally {
		}
		return 0;
	}

	@Override
	public int write(String path, long fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
		// SDFSLogger.getLog().info("writing " + path);
		// SDFSLogger.getLog().debug("writing " + buf.capacity() + " to " +
		// path);
		// Thread.currentThread().setName("21
		// "+Long.toString(System.currentTimeMillis()));
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		try {
			if (Main.volume.isFull())
				throw new FuseException("Volume Full").initErrno(Errno.ENOSPC);
			/*
			 * log.info("writing data to  " +path + " at " + offset + " and length of " +
			 * buf.capacity());
			 */
			// byte[] b = new byte[buf.capacity()];
			// buf.position(0);
			// buf.get(b);
			// buf.position(0);
			// SDFSLogger.getLog().info("writing " + path + " len" + buf.capacity() + "==" +
			// new String(b) + "==/n/n");

			DedupFileChannel ch = this.getFileChannel(path, (Long) fh, -1);
			try {
				ch.writeFile(buf, buf.capacity(), 0, offset, true);
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to write to file" + path, e);
				throw new FuseException().initErrno(Errno.EACCES);
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().debug(path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		}
		return 0;
	}

	private File resolvePath(String path) throws FuseException {
		String pt = mountedVolume + path;
		File _f = new File(pt);
		if (Files.isSymbolicLink(_f.toPath())) {
			try {
				_f = Files.readSymbolicLink(_f.toPath()).toFile();
			} catch (IOException e) {
				SDFSLogger.getLog().debug("error resolving " + mountedVolume + path, e);
				throw new FuseException().initErrno(Errno.EBADFD);
			}
		}
		try {
			this.checkInFS(_f);
		} catch (FuseException e) {
			SDFSLogger.getLog().warn("unable", e);
			throw e;
		}
		if (!_f.exists()) {
			_f = null;
			SDFSLogger.getLog().debug("no such node " + mountedVolume + path);
			throw new FuseException().initErrno(Errno.ENOENT);
		}
		return _f;
	}

	private int getFtype(File _f) throws FuseException {

		if (!_f.exists()) {

			try {
				Path p = Paths.get(_f.getCanonicalPath());
				if (Files.isSymbolicLink(p)) {
					return FuseFtypeConstants.TYPE_SYMLINK;
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn(e);
			}
			SDFSLogger.getLog().debug(_f.getPath() + " does not exist");
			throw new FuseException().initErrno(Errno.ENOENT);

		}

		try {
			Path p = Paths.get(_f.getCanonicalPath());
			boolean isSymbolicLink = Files.isSymbolicLink(p);
			if (isSymbolicLink)
				return FuseFtypeConstants.TYPE_SYMLINK;
			else if (_f.isDirectory())
				return FuseFtypeConstants.TYPE_DIR;
			else if (_f.isFile())
				return FuseFtypeConstants.TYPE_FILE;
		} catch (Exception e) {
			SDFSLogger.getLog().warn(_f.getPath() + " does not exist", e);
			_f = null;
			throw new FuseException("No such node").initErrno(Errno.ENOENT);
		}
		throw new FuseException().initErrno(Errno.ENOENT);
	}

	private int getFtype(String path) throws FuseException {
		// SDFSLogger.getLog().info("Path=" + path);
		String pt = mountedVolume + path;
		File _f = new File(pt);
		/*
		try {
			this.checkInFS(_f);
		} catch (FuseException e) {
			SDFSLogger.getLog().warn("unable", e);
			throw e;
		}
		*/
		try {
			if (!Files.exists(Paths.get(_f.getCanonicalPath()), LinkOption.NOFOLLOW_LINKS)) {
				throw new FuseException().initErrno(Errno.ENOENT);
			}
			Path p = _f.toPath();

			boolean isSymbolicLink = Files.isSymbolicLink(p);
			if (isSymbolicLink)
				return FuseFtypeConstants.TYPE_SYMLINK;
			else if (_f.isDirectory())
				return FuseFtypeConstants.TYPE_DIR;
			else if (_f.isFile()) {
				return FuseFtypeConstants.TYPE_FILE;
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			_f = null;
			SDFSLogger.getLog().warn(path + " does not exist", e);
			throw new FuseException("No such node").initErrno(Errno.ENOENT);
		}
		SDFSLogger.getLog().error("could not determine type for " + path);
		throw new FuseException().initErrno(Errno.ENOENT);
	}

	private DedupFileChannel getFileChannel(String path, int flags) throws FuseException {
		File f = this.resolvePath(path);

		try {
			MetaDataDedupFile mf = MetaFileStore.getMF(f);
			return mf.getDedupFile(false).getChannel(flags);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to open file" + path, e);
			throw new FuseException("error opening " + path).initErrno(Errno.EACCES);
		}
	}

	public int getxattr(String path, String name, ByteBuffer dst) throws FuseException, BufferOverflowException {
		this.resolvePath(path);
		try {
			int ftype = this.getFtype(path);
			if (ftype != FuseFtypeConstants.TYPE_SYMLINK) {
				if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs.") || name.startsWith("user.dse"))
					dst.put(sdfsCmds.getAttr(name, path).getBytes());
				else {
					File f = this.resolvePath(path);
					MetaDataDedupFile mf = MetaFileStore.getMF(f);
					String st = mf.getXAttribute(name);
					if (st != null)
						dst.put(st.getBytes());
					else
						throw new FuseException().initErrno(Errno.ENODATA);

				}
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().debug("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {
		}
		return 0;
	}

	@Override
	public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter) throws FuseException {
		// SDFSLogger.getLog().info("19ddddd3");
		this.resolvePath(path);
		try {
			if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs") || name.startsWith("user.dse")) {
				try {
					sizeSetter.setSize(sdfsCmds.getAttr(name, path).getBytes().length);
				} catch (Exception e) {
					SDFSLogger.getLog().error("attribure get error for " + name, e);
				}
			} else {
				File f = this.resolvePath(path);

				MetaDataDedupFile mf = MetaFileStore.getMF(f);
				String st = mf.getXAttribute(name);
				if (st != null)
					sizeSetter.setSize(st.getBytes().length);
				else
					throw new FuseException().initErrno(Errno.ENODATA);

			}

		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting exattr size for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {
		}
		return 0;
	}

	@Override
	public int listxattr(String path, XattrLister lister) throws FuseException {
		try {
			// sdfsCmds.listAttrs(lister);
			File f = this.resolvePath(path);
			if (!f.exists())
				throw new FuseException().initErrno(Errno.ENFILE);
			MetaDataDedupFile mf = MetaFileStore.getMF(f);
			for (String s : mf.getXAttersNames()) {
				lister.add(s);
			}

		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().debug("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {

		}
		return 0;
	}

	@Override
	public int removexattr(String path, String name) throws FuseException {
		try {
			File f = this.resolvePath(path);
			if (!f.exists())
				throw new FuseException().initErrno(Errno.ENFILE);
			MetaDataDedupFile mf = MetaFileStore.getMF(f);
			mf.removeXAttribute(name);
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error removing exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {
		}
		return 0;
	}

	public int setxattr(String path, String name, ByteBuffer value, int flags) throws FuseException {
		try {
			byte valB[] = new byte[value.capacity()];
			value.get(valB);
			String valStr = new String(valB);
			if (!StringUtils.checkIfString(valB)) {
				valStr = "!Base64:" + BaseEncoding.base64().encode(valB);
			}

			if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs.") || name.startsWith("user.dse")) {
				sdfsCmds.runCMD(path, name, valStr);
			} else {
				File f = this.resolvePath(path);
				MetaDataDedupFile mf = MetaFileStore.getMF(f);
				mf.addXAttribute(name, valStr);
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("set " + name + " to " + valStr);
				if (mf.isFile())
					mf.setDirty(true);

			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {
		}
		return 0;
	}

	@Override
	public int getxattr(String path, String name, ByteBuffer dst, int position)
			throws FuseException, BufferOverflowException {
		// Thread.currentThread().setName("21
		// "+Long.toString(System.currentTimeMillis()));
		// SDFSLogger.getLog().info("19dddacaea3");
		this.resolvePath(path);
		try {
			int ftype = this.getFtype(path);
			if (ftype != FuseFtypeConstants.TYPE_SYMLINK) {
				if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs.") || name.startsWith("user.dse"))
					dst.put(sdfsCmds.getAttr(name, path).getBytes());
				else {
					File f = this.resolvePath(path);
					MetaDataDedupFile mf = MetaFileStore.getMF(f);
					String st = mf.getXAttribute(name);

					if (st != null) {
						if (st.startsWith("!Base64:")) {
							String mk = st.substring("!Base64:".length());
							dst.put(BaseEncoding.base64().decode(mk));
						} else
							dst.put(st.getBytes());
					} else
						throw new FuseException().initErrno(Errno.ENODATA);

				}
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {

		}
		return 0;
	}

	@Override
	public int setxattr(String path, String name, ByteBuffer value, int flags, int position) throws FuseException {
		// SDFSLogger.getLog().info("aaa193");
		this.resolvePath(path);
		try {
			byte valB[] = new byte[value.capacity()];
			value.get(valB);
			String valStr = new String(valB);
			if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs.") || name.startsWith("user.dse")) {
				sdfsCmds.runCMD(path, name, valStr);
			} else {
				File f = this.resolvePath(path);
				MetaDataDedupFile mf = MetaFileStore.getMF(f);
				mf.addXAttribute(name, valStr);
				if (mf.isFile())
					mf.setDirty(true);
				eventBus.post(new MFileWritten(mf, true));
			}
		} catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {

		}
		return 0;
	}

	@Override
	public void destroy() throws FuseException {
		try {
			SDFSLogger.getLog().info("unmount initiated");
			MountSDFS.shutdownHook.shutdown();
		} catch (Exception e) {
			SDFSLogger.getLog().error("error shutting down service ", e);
			throw new FuseException().initErrno(Errno.ENODATA);
		}
	}

}