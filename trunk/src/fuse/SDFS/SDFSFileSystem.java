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
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.List;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.events.MFileDeleted;
import org.opendedup.sdfs.io.events.MFileWritten;

import com.google.common.eventbus.EventBus;

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
	private static final int BLOCK_SIZE = 32768;
	private static final int NAME_LENGTH = 2048;
	static long tbc = 1099511627776L;
	static int gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;
	static int kbc = 1024;
	private SDFSCmds sdfsCmds;
	private static EventBus eventBus = new EventBus();
	
	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

	public SDFSFileSystem(String mountedVolume, String mountPoint) {

		SDFSLogger.getLog().info(
				"mounting " + mountedVolume + " to " + mountPoint);
		this.mountedVolume = mountedVolume;
		if (!this.mountedVolume.endsWith("/"))
			this.mountedVolume = this.mountedVolume + "/";
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
			if (ftype == FuseFtypeConstants.TYPE_SYMLINK
					|| ftype == FuseFtypeConstants.TYPE_DIR) {
				Path p = Paths.get(f.getPath());

				try {
					Files.setAttribute(p, "unix:mode", Integer.valueOf(mode),
							LinkOption.NOFOLLOW_LINKS);
				} catch (IOException e) {
					SDFSLogger.getLog().warn("access denied for " + path, e);
					throw new FuseException("access denied for " + path)
							.initErrno(Errno.EACCES);
				} finally {
					path = null;
				}
			} else {

				try {
					MetaDataDedupFile mf = MetaFileStore.getMF(f);
					mf.setMode(mode);
				} catch (Exception e) {
					SDFSLogger.getLog().warn("access denied for " + path, e);
					throw new FuseException("access denied for " + path)
							.initErrno(Errno.EACCES);
				} finally {
				}
			}
		} finally {
		}
		return 0;
	}

	@Override
	public int chown(String path, int uid, int gid) throws FuseException {
		// SDFSLogger.getLog().info("17");
		try {
			File f = resolvePath(path);
			int ftype = this.getFtype(path);
			if (ftype == FuseFtypeConstants.TYPE_SYMLINK
					|| ftype == FuseFtypeConstants.TYPE_DIR) {
				Path p = Paths.get(f.getPath());
				try {
					Files.setAttribute(p, "unix:uid", Integer.valueOf(uid),
							LinkOption.NOFOLLOW_LINKS);
					Files.setAttribute(p, "unix:gid", Integer.valueOf(gid),
							LinkOption.NOFOLLOW_LINKS);
				} catch (IOException e) {
					e.printStackTrace();
					throw new FuseException("access denied for " + path)
							.initErrno(Errno.EACCES);
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
					throw new FuseException("access denied for " + path)
							.initErrno(Errno.EACCES);
				} finally {

				}
			}
		} finally {
		}
		return 0;
	}

	@Override
	public int flush(String path, Object fh) throws FuseException {
		// SDFSLogger.getLog().info("109");
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		DedupFileChannel ch = (DedupFileChannel) fh;
		try {
			ch.force(true);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to sync file [" + path + "]", e);
			throw new FuseException("unable to sync file")
					.initErrno(Errno.EACCES);
		} finally {
		}
		return 0;
	}

	@Override
	public int fsync(String path, Object fh, boolean isDatasync)
			throws FuseException {
		// SDFSLogger.getLog().info("1000");
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		DedupFileChannel ch = (DedupFileChannel) fh;
		try {
			if (Main.safeSync) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"sync " + path + " df="
									+ ch.getDedupFile().getGUID());
				ch.force(true);
			}

		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to sync file [" + path + "]", e);
			throw new FuseException("unable to sync file")
					.initErrno(Errno.EACCES);
		} finally {

		}
		return 0;
	}

	@Override
	public int getattr(String path, FuseGetattrSetter getattrSetter)
			throws FuseException {
		// SDFSLogger.getLog().info("0");
		try {
			int ftype = this.getFtype(path);
			// SDFSLogger.getLog().info("poop " + path + " " + ftype);
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
						uid = (Integer) Files.getAttribute(p, "unix:uid",
								LinkOption.NOFOLLOW_LINKS);
						gid = (Integer) Files.getAttribute(p, "unix:gid",
								LinkOption.NOFOLLOW_LINKS);
						mode = (Integer) Files.getAttribute(p, "unix:mode",
								LinkOption.NOFOLLOW_LINKS);
					} catch (Exception e) {
						SDFSLogger.getLog().error(
								"unable to parse sylink " + path, e);
					}

					int atime = 0;
					int ctime = 0;
					int mtime = 0;
					long fileLength = 0;
					attrs = Files.readAttributes(p, BasicFileAttributes.class,
							LinkOption.NOFOLLOW_LINKS);
					fileLength = attrs.size();
					mtime = (int) (attrs.lastModifiedTime().toMillis() / 1000L);
					atime = (int) (attrs.lastAccessTime().toMillis() / 1000L);
					ctime = (int) (attrs.creationTime().toMillis() / 1000L);

					getattrSetter.set(p.hashCode(), mode, 1, uid, gid, 0,
							fileLength,
							(fileLength * NAME_LENGTH + BLOCK_SIZE - 1)
									/ BLOCK_SIZE, atime, mtime, ctime);
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to parse sylink " + path,
							e);
					throw new FuseException().initErrno(Errno.EACCES);
				} finally {
					attrs = null;
					p = null;
				}
			} else {
				File f = resolvePath(path);
				Path p = null;
				try {
					p = Paths.get(f.getPath());
					if (ftype == FuseFtypeConstants.TYPE_DIR) {
						int uid = (Integer) Files.getAttribute(p, "unix:uid");
						int gid = (Integer) Files.getAttribute(p, "unix:gid");
						int mode = (Integer) Files.getAttribute(p, "unix:mode");
						MetaDataDedupFile mf = MetaFileStore.getFolder(f);
						int atime = (int) (mf.getLastAccessed() / 1000L);
						int mtime = (int) (mf.lastModified() / 1000L);
						int ctime = (int) (0 / 1000L);

						long fileLength = f.length();
						getattrSetter.set(mf.getGUID().hashCode(), mode, 1,
								uid, gid, 0, fileLength * NAME_LENGTH,
								(fileLength * NAME_LENGTH + BLOCK_SIZE - 1)
										/ BLOCK_SIZE, atime, mtime, ctime);
					} else {
						MetaDataDedupFile mf = MetaFileStore.getMF(f);
						int uid = mf.getOwner_id();
						int gid = mf.getGroup_id();
						int mode = mf.getMode();
						int atime = (int) (mf.getLastAccessed() / 1000L);
						int ctime = (int) (0 / 1000L);
						int mtime = (int) (mf.lastModified() / 1000L);
						long fileLength = mf.length();
						long actualBytes = (mf.getIOMonitor()
								.getActualBytesWritten() * 2) / 1024;
						if (actualBytes == 0
								&& mf.getIOMonitor().getActualBytesWritten() > 0)
							actualBytes = (Main.CHUNK_LENGTH * 2) / 1024;
						getattrSetter.set(mf.getGUID().hashCode(), mode, 1,
								uid, gid, 0, fileLength, actualBytes, atime,
								mtime, ctime);
					}
				} catch (Exception e) {
					SDFSLogger.getLog().error(
							"unable to parse attributes " + path
									+ " at physical path " + f.getPath(), e);
					throw new FuseException().initErrno(Errno.EACCES);
				} finally {
					f = null;
					p = null;
				}
			}
		} finally {

		}
		return 0;
	}

	@Override
	public int getdir(String path, FuseDirFiller dirFiller)
			throws FuseException {
		// SDFSLogger.getLog().info("1");
		try {
			File f = null;
			try {
				f = resolvePath(path);
				File[] mfs = f.listFiles();
				dirFiller.add(".", ".".hashCode(), FuseFtypeConstants.TYPE_DIR);
				dirFiller.add("..", "..".hashCode(),
						FuseFtypeConstants.TYPE_DIR);
				for (int i = 0; i < mfs.length; i++) {
					File _mf = mfs[i];
					dirFiller.add(_mf.getName(), _mf.hashCode(),
							this.getFtype(_mf));
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to read path " + path, e);
				throw new FuseException().initErrno(Errno.EACCES);

			} finally {
				f = null;
			}
		} finally {
		}
		return 0;
	}

	@Override
	public int link(String from, String to) throws FuseException {
		// SDFSLogger.getLog().info("2");
		throw new FuseException("error hard linking is not supported")
				.initErrno(Errno.ENOSYS);

		/*
		 * log.debug("link(): " + from + " to " + to); File dst = new
		 * File(mountedVolume + to); if (dst.exists()) { throw new
		 * FuseException("file exists") .initErrno(FuseException.EPERM); } Path
		 * srcP = Paths.get(from); Path dstP = Paths.get(dst.getPath()); try {
		 * dstP.createLink(srcP); } catch (IOException e) {
		 * log.error("error linking " + from + " to " + to, e); throw new
		 * FuseException("error linking " + from + " to " + to)
		 * .initErrno(FuseException.EACCES); } finally { srcP = null; dstP =
		 * null; } return 0;
		 */
	}

	@Override
	public int mkdir(String path, int mode) throws FuseException {
		// SDFSLogger.getLog().info("3");
		try {
			File f = new File(this.mountedVolume + path);
			if (Main.volume.isOffLine())
				throw new FuseException("volume offline")
						.initErrno(Errno.ENAVAIL);
			if (Main.volume.isFull())
				throw new FuseException("Volume Full").initErrno(Errno.ENOSPC);
			if (f.exists()) {
				f = null;
				throw new FuseException("folder exists").initErrno(Errno.EPERM);
			}
			try {
				MetaFileStore.mkDir(f, mode);
				eventBus.post(new MFileWritten(MetaFileStore.getMF(f)));
			} catch (IOException e) {
				SDFSLogger.getLog().error("error while making dir " + path, e);
				throw new FuseException("access denied for " + path)
						.initErrno(Errno.EACCES);
			} finally {
				path = null;
			}
		} finally {
		}
		return 0;
	}

	@Override
	public int mknod(String path, int mode, int rdev) throws FuseException {
		// SDFSLogger.getLog().info("4");
		try {
			File f = new File(this.mountedVolume + path);
			if (Main.volume.isOffLine())
				throw new FuseException("volume offline")
						.initErrno(Errno.ENAVAIL);
			if (Main.volume.isFull())
				throw new FuseException("Volume Full").initErrno(Errno.ENOSPC);
			if (f.exists()) {
				f = null;
				throw new FuseException("file exists").initErrno(Errno.EPERM);
			} else {
				MetaDataDedupFile mf = MetaFileStore.getMF(f);
				mf.sync();
				// Wait up to 5 seconds for file to be created
				int z = 5000;
				int i = 0;
				while (!f.exists()) {
					i++;
					if (i == z) {
						throw new FuseException("file creation timed out for "
								+ path).initErrno(Errno.EBUSY);
					} else {
						try {
							Thread.sleep(1);
						} catch (InterruptedException e) {
							throw new FuseException(
									"file creation interrupted for " + path)
									.initErrno(Errno.EACCES);
						}
					}

				}
				try {
					mf.setMode(mode);
				} finally {
					f = null;
				}

			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("error making " + path, e);
			throw new FuseException("access denied for " + path)
					.initErrno(Errno.EACCES);
		}
		return 0;
	}

	@Override
	public int open(String path, int flags, FuseOpenSetter openSetter)
			throws FuseException {
		// SDFSLogger.getLog().info("9");
		// Thread.currentThread().setName("10 " +
		// Long.toString(System.currentTimeMillis()));
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		try {
			openSetter.setFh(this.getFileChannel(path, flags));
		} catch (FuseException e) {
			SDFSLogger.getLog().error("error while opening file", e);
			throw e;
		} finally {
		}
		return 0;
	}

	@Override
	public int read(String path, Object fh, ByteBuffer buf, long offset)
			throws FuseException {
		// SDFSLogger.getLog().info("1911");
		if (Main.volume.isOffLine())
			throw new FuseException("Volume Offline").initErrno(Errno.ENODEV);
		try {
			DedupFileChannel ch = (DedupFileChannel) fh;
			int read = ch.read(buf, 0, buf.capacity(), offset);
			if (read == -1)
				read = 0;
		} catch (DataArchivedException e) {
			throw new FuseException("File Archived").initErrno(Errno.ENODATA);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to read file " + path, e);
			throw new FuseException("error reading " + path)
					.initErrno(Errno.ENODATA);
		} finally {
		}
		return 0;
	}

	@Override
	public int readlink(String path, CharBuffer link) throws FuseException {
		// SDFSLogger.getLog().info("190");
		Path p = Paths.get(this.mountedVolume + path);
		try {

			String lpath = Files.readSymbolicLink(p).toString();
			if (lpath.startsWith(this.mountedVolume))
				lpath = this.mountPoint
						+ lpath.substring(this.mountedVolume.length());
			// SDFSLogger.getLog().info("path=" + path + " lpath=" + lpath);
			link.put(lpath);
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting linking " + path, e);
			throw new FuseException("error getting linking " + path)
					.initErrno(Errno.EACCES);
		} finally {
			p = null;
		}
		return 0;
	}

	@Override
	public int release(String path, Object fh, int flags) throws FuseException {
		// SDFSLogger.getLog().info("199");
		try {
			if (!Main.safeClose)
				return 0;
			DedupFileChannel ch = (DedupFileChannel) fh;
			try {
				ch.getDedupFile().unRegisterChannel(ch, flags);
				fh = null;
				ch = null;
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to close " + path, e);
				throw new FuseException("error getting linking " + path)
						.initErrno(Errno.EBADFD);
			}
		} finally {
		}
		return 0;
	}

	@Override
	public int rename(String from, String to) throws FuseException {
		// SDFSLogger.getLog().info("198");
		try {
			File f = null;

			try {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"renaming [" + from + "] to [" + to + "]");
				f = resolvePath(from);
				MetaFileStore.rename(f.getPath(), this.mountedVolume + to);
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to rename " + from + " to " + to, e);
				throw new FuseException().initErrno(Errno.EACCES);
			} finally {
				f = null;
			}
		} finally {
		}
		return 0;
	}

	@Override
	public int rmdir(String path) throws FuseException {
		// SDFSLogger.getLog().info("197");
		try {
			if (this.getFtype(path) == FuseFtypeConstants.TYPE_SYMLINK) {

				File f = new File(mountedVolume + path);
				// SDFSLogger.getLog().info("deleting symlink " + f.getPath());
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
						boolean del = MetaFileStore.removeMetaFile(f.getPath());
						if (del)
							return 0;
						else {
							if (SDFSLogger.isDebug())
								SDFSLogger.getLog().debug(
										"unable to delete folder "
												+ f.getPath());
							throw new FuseException()
									.initErrno(Errno.ENOTEMPTY);
						}

					} catch (Exception e) {
						SDFSLogger.getLog().debug(
								"unable to delete folder " + f.getPath());
						throw new FuseException().initErrno(Errno.EACCES);
					}

				}
			}
		} finally {
		}
	}

	@Override
	public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
		// SDFSLogger.getLog().info("196");
		try {
			int blocks = (int) Main.volume.getTotalBlocks();
			int used = (int) Main.volume.getUsedBlocks();
			if (used > blocks)
				used = blocks;
			statfsSetter.set(Main.volume.getBlockSize(), blocks, blocks - used,
					blocks - used, 0, 0, NAME_LENGTH);
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

			if (from.startsWith(this.mountPoint)) {
				from = from.substring(mountPoint.length());
				this.resolvePath(from);
				src = new File(mountedVolume + from);
			} else if (!Main.allowExternalSymlinks) {
				SDFSLogger.getLog().error(
						"external symlinks are not allowed " + from + " to "
								+ to);
				throw new FuseException().initErrno(Errno.EACCES);
			} else {
				src = new File(from);
			}
			File dst = new File(mountedVolume + to);
			if (dst.exists()) {
				throw new FuseException().initErrno(Errno.EPERM);
			}
			Path srcP = Paths.get(src.getPath());
			Path dstP = Paths.get(dst.getPath());
			// SDFSLogger.getLog().info(
			// "symlink " + src.getPath() + " to " + dst.getPath());
			try {
				Files.createSymbolicLink(dstP, srcP);
				SDFSLogger.getLog().info("zzzz=" + dst.getPath() + " " + MetaFileStore.getMF(dst.getPath()).isSymlink());
				eventBus.post(new MFileWritten(MetaFileStore.getMF(dst.getPath())));
			} catch (IOException e) {

				SDFSLogger.getLog().error(
						"error linking " + from + " to " + to, e);
				throw new FuseException().initErrno(Errno.EACCES);
			}
		} finally {
		}
		return 0;
	}

	@Override
	public int truncate(String path, long size) throws FuseException {
		// SDFSLogger.getLog().info("193");
		try {
			DedupFileChannel ch = this.getFileChannel(path, -1);
			ch.truncateFile(size);
			ch.getDedupFile().unRegisterChannel(ch, -1);
		} catch (IOException e) {
			SDFSLogger.getLog().error("unable to truncate file " + path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		} finally {
		}
		return 0;
	}

	@Override
	public int unlink(String path) throws FuseException {
		// Thread.currentThread().setName("19 "+Long.toString(System.currentTimeMillis()));
		try {
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("removing " + path);
			if (!Main.safeClose) {
				try {
					this.getFileChannel(path, -1).getDedupFile().forceClose();
				} catch (IOException e) {
					SDFSLogger.getLog()
							.error("unable to close file " + path, e);
				}
			}
			if (this.getFtype(path) == FuseFtypeConstants.TYPE_SYMLINK) {
				Path p = new File(mountedVolume + path).toPath();
				// SDFSLogger.getLog().info("deleting symlink " + f.getPath());
				try {
					MetaDataDedupFile mf = MetaFileStore.getMF(this.resolvePath(path));
					eventBus.post(new MFileDeleted(mf));
					if (!Files.deleteIfExists(p)) {
						eventBus.post(new MFileWritten(mf));
						throw new FuseException().initErrno(Errno.EACCES);
					}
					
					return 0;
				} catch (IOException e) {
					SDFSLogger.getLog().warn("unable to delete symlink " + p);
					throw new FuseException().initErrno(Errno.ENOSYS);
				}
			} else {

				File f = this.resolvePath(path);
				// SDFSLogger.getLog().info("deleting file " + f.getPath());
				try {
					if (MetaFileStore.removeMetaFile(f.getPath()))
						return 0;
					else {
						SDFSLogger.getLog().warn(
								"unable to delete folder " + f.getPath());
						throw new FuseException().initErrno(Errno.ENOSYS);
					}
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to file file " + path, e);
					throw new FuseException().initErrno(Errno.EACCES);
				}
			}
		} finally {
		}
	}

	@Override
	public int utime(String path, int atime, int mtime) throws FuseException {
		// SDFSLogger.getLog().info("19");
		// Thread.currentThread().setName("20 "+Long.toString(System.currentTimeMillis()));
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
				if(mf.isFile())
					mf.setDirty(true);
				eventBus.post(new MFileWritten(mf));
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable change utime path " + path, e);
			throw new FuseException().initErrno(Errno.EACCES);
		} finally {
		}
		return 0;
	}

	@Override
	public int write(String path, Object fh, boolean isWritepage,
			ByteBuffer buf, long offset) throws FuseException {
		// SDFSLogger.getLog().info("191");
		// SDFSLogger.getLog().debug("writing " + buf.capacity());
		// Thread.currentThread().setName("21 "+Long.toString(System.currentTimeMillis()));
		if (Main.volume.isOffLine())
			throw new FuseException("volume offline").initErrno(Errno.ENAVAIL);
		try {
			if (Main.volume.isFull())
				throw new FuseException("Volume Full").initErrno(Errno.ENOSPC);
			/*
			 * log.info("writing data to  " +path + " at " + offset +
			 * " and length of " + buf.capacity());
			 */
			DedupFileChannel ch = (DedupFileChannel) fh;
			try {
				ch.writeFile(buf, buf.capacity(), 0, offset, true);
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to write to file" + path, e);
				throw new FuseException().initErrno(Errno.EACCES);
			}
		} finally {
		}
		return 0;
	}

	private File resolvePath(String path) throws FuseException {
		String pt = mountedVolume + path;
		File _f = new File(pt);

		if (!_f.exists()) {
			_f = null;
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("No such node");
			SDFSLogger.getLog().error("no such node " + path);
			throw new FuseException().initErrno(Errno.ENOENT);
		}
		return _f;
	}

	private int getFtype(File _f) throws FuseException {

		if (!_f.exists()) {
			Path p = Paths.get(_f.getPath());
			try {
				if (Files.isSymbolicLink(p)) {
					return FuseFtypeConstants.TYPE_SYMLINK;
				}
			} catch (Exception e) {
				SDFSLogger.getLog().warn(e);
			}
			SDFSLogger.getLog().warn(_f.getPath() + " does not exist");
			throw new FuseException().initErrno(Errno.ENOENT);

		}
		Path p = Paths.get(_f.getPath());
		try {
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
			p = null;
			throw new FuseException("No such node").initErrno(Errno.ENOENT);
		}
		throw new FuseException().initErrno(Errno.ENOENT);
	}

	private int getFtype(String path) throws FuseException {
		String pt = mountedVolume + path;
		File _f = new File(pt);

		if (!Files.exists(Paths.get(_f.getPath()), LinkOption.NOFOLLOW_LINKS)) {
			throw new FuseException().initErrno(Errno.ENOENT);
		}
		Path p = Paths.get(_f.getPath());
		try {
			boolean isSymbolicLink = Files.isSymbolicLink(p);
			if (isSymbolicLink)
				return FuseFtypeConstants.TYPE_SYMLINK;
			else if (_f.isDirectory())
				return FuseFtypeConstants.TYPE_DIR;
			else if (_f.isFile()) {
				return FuseFtypeConstants.TYPE_FILE;
			}
		} catch (Exception e) {
			_f = null;
			p = null;
			SDFSLogger.getLog().warn(path + " does not exist", e);
			throw new FuseException("No such node").initErrno(Errno.ENOENT);
		}
		SDFSLogger.getLog().error("could not determine type for " + path);
		throw new FuseException().initErrno(Errno.ENOENT);
	}

	private DedupFileChannel getFileChannel(String path, int flags)
			throws FuseException {
		File f = this.resolvePath(path);

		try {
			MetaDataDedupFile mf = MetaFileStore.getMF(f);
			return mf.getDedupFile().getChannel(flags);
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to open file" + path, e);
			throw new FuseException("error opening " + path)
					.initErrno(Errno.EACCES);
		}
	}

	public int getxattr(String path, String name, ByteBuffer dst)
			throws FuseException, BufferOverflowException {
		this.resolvePath(path);
		try {
			int ftype = this.getFtype(path);
			if (ftype != FuseFtypeConstants.TYPE_SYMLINK) {
				if (name.startsWith("user.cmd.")
						|| name.startsWith("user.sdfs.")
						|| name.startsWith("user.dse"))
					dst.put(sdfsCmds.getAttr(name, path).getBytes());
				else {
					File f = this.resolvePath(path);
						Path _p = f.toPath();
						UserDefinedFileAttributeView view = Files.getFileAttributeView(_p,
						        UserDefinedFileAttributeView.class);
						view.read(name, dst);
					
				}
			}
		} catch(java.nio.file.FileSystemException e) {
			if(SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		}catch (Exception e) {
			SDFSLogger.getLog().error("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {
		}
		return 0;
	}

	@Override
	public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter)
			throws FuseException {
		this.resolvePath(path);
		try {
			if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs")
					|| name.startsWith("user.dse")) {
				try {
					sizeSetter
							.setSize(sdfsCmds.getAttr(name, path).getBytes().length);
				} catch (Exception e) {
					SDFSLogger.getLog().error(
							"attribure get error for " + name, e);
				}
			} else {
				File f = this.resolvePath(path);
				
					Path _p = f.toPath();
					UserDefinedFileAttributeView view = Files.getFileAttributeView(_p,
					        UserDefinedFileAttributeView.class);
					sizeSetter.setSize(view.size(name));
				
			}
			
		} catch(java.nio.file.FileSystemException e) {
			if(SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		}catch (Exception e) {
			SDFSLogger.getLog().error("error getting exattr for " + path, e);
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
				Path _p = f.toPath();
				UserDefinedFileAttributeView view = Files.getFileAttributeView(_p,
				        UserDefinedFileAttributeView.class);
				List<String> l = view.list();
				for(String s : l) {
					lister.add(s);
				}
		
			
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {

		}
		return 0;
	}

	@Override
	public int removexattr(String path, String name) throws FuseException {
		try {

		} finally {
		}
		return 0;
	}

	public int setxattr(String path, String name, ByteBuffer value, int flags)
			throws FuseException {
		// Thread.currentThread().setName("25 "+Long.toString(System.currentTimeMillis()));
		try {
			byte valB[] = new byte[value.capacity()];
			value.get(valB);
			String valStr = new String(valB);
			if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs.")
					|| name.startsWith("user.dse")) {
				sdfsCmds.runCMD(path, name, valStr);
			} else {
				File f = this.resolvePath(path);
					Path _path = f.toPath();
					UserDefinedFileAttributeView view = Files
							.getFileAttributeView(_path,
									UserDefinedFileAttributeView.class);
					view.write(name, value);
					if(SDFSLogger.isDebug())
						SDFSLogger.getLog().debug("set " + name + " to " + valStr);
					MetaDataDedupFile mf = MetaFileStore.getMF(f);
					if(mf.isFile())
						mf.setDirty(true);
					eventBus.post(new MFileWritten(mf));
					
				
			}
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
		// Thread.currentThread().setName("21 "+Long.toString(System.currentTimeMillis()));
		this.resolvePath(path);
		try {
			int ftype = this.getFtype(path);
			if (ftype != FuseFtypeConstants.TYPE_SYMLINK) {
				if (name.startsWith("user.cmd.")
						|| name.startsWith("user.sdfs.")
						|| name.startsWith("user.dse"))
					dst.put(sdfsCmds.getAttr(name, path).getBytes());
				else {
					File f = this.resolvePath(path);
						Path _p = f.toPath();
						UserDefinedFileAttributeView view = Files.getFileAttributeView(_p,
						        UserDefinedFileAttributeView.class);
						view.read(name, dst);
					
				}
			}
		} catch(java.nio.file.FileSystemException e) {
			if(SDFSLogger.isDebug())
				SDFSLogger.getLog().debug("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		}
		catch (FuseException e) {
			throw e;
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {

		}
		return 0;
	}

	@Override
	public int setxattr(String path, String name, ByteBuffer value, int flags,
			int position) throws FuseException {
		this.resolvePath(path);
		try {
			byte valB[] = new byte[value.capacity()];
			value.get(valB);
			String valStr = new String(valB);
			if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs.")
					|| name.startsWith("user.dse")) {
				sdfsCmds.runCMD(path, name, valStr);
			} else {
				File f = this.resolvePath(path);
				Path _path = f.toPath();
				UserDefinedFileAttributeView view = Files
						.getFileAttributeView(_path,
								UserDefinedFileAttributeView.class);
				view.write(name, value);
				MetaDataDedupFile mf = MetaFileStore.getMF(f);
				if(mf.isFile())
					mf.setDirty(true);
				eventBus.post(new MFileWritten(mf));
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("error getting exattr for " + path, e);
			throw new FuseException().initErrno(Errno.ENODATA);
		} finally {

		}
		return 0;
	}

}