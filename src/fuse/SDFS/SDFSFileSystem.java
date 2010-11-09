package fuse.SDFS;

import java.io.File;


import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.Attributes;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;

import fuse.Filesystem3;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseFtype;
import fuse.FuseGetattrSetter;
import fuse.FuseOpenSetter;
import fuse.FuseSizeSetter;
import fuse.FuseStatfsSetter;
import fuse.XattrLister;
import fuse.XattrSupport;

public class SDFSFileSystem implements Filesystem3, XattrSupport {

	public String mountedVolume;
	public String mountPoint;
	private static final Log log = LogFactory.getLog(SDFSFileSystem.class);
	private static final int BLOCK_SIZE = 32768;
	private static final int NAME_LENGTH = 2048;
	static long tbc = 1099511627776L;
	static int gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;
	static int kbc = 1024;
	private SDFSCmds sdfsCmds;

	public SDFSFileSystem(String mountedVolume, String mountPoint) {

		log.info("mounting " + mountedVolume + " to " + mountPoint);
		this.mountedVolume = mountedVolume;
		this.mountPoint = mountPoint;
		sdfsCmds = new SDFSCmds(this.mountedVolume, this.mountPoint);
		File f = new File(this.mountedVolume);
		if (!f.exists())
			f.mkdirs();
	}

	public int chmod(String path, int mode) throws FuseException {
		// log.info("setting file permissions " + mode);
		//SDFSLogger.getLog().info("4");
		File f = resolvePath(path);
		int ftype = this.getFtype(path);
		if (ftype == FuseFtype.TYPE_SYMLINK || ftype == FuseFtype.TYPE_DIR) {
			Path p = Paths.get(f.getPath());

			try {
				p.setAttribute("unix:mode", Integer.valueOf(mode));
			} catch (IOException e) {
				e.printStackTrace();
				throw new FuseException("access denied for " + path)
						.initErrno(FuseException.EACCES);
			} finally {
				path = null;
			}
		} else {
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			try {
				mf.setMode(mode);
			} catch (IOException e) {
				e.printStackTrace();
				throw new FuseException("access denied for " + path)
						.initErrno(FuseException.EACCES);
			} finally {
			}
		}
		return 0;
	}

	public int chown(String path, int uid, int gid) throws FuseException {
		//SDFSLogger.getLog().info("3");
		File f = resolvePath(path);
		int ftype = this.getFtype(path);
		if (ftype == FuseFtype.TYPE_SYMLINK || ftype == FuseFtype.TYPE_DIR) {
			Path p = Paths.get(f.getPath());
			try {
				p.setAttribute("unix:uid", Integer.valueOf(uid));
				p.setAttribute("unix:gid", Integer.valueOf(gid));
			} catch (IOException e) {
				e.printStackTrace();
				throw new FuseException("access denied for " + path)
						.initErrno(FuseException.EACCES);
			} finally {
				path = null;
			}
		} else {
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			try {
				mf.setOwner_id(uid);
				mf.setGroup_id(gid);
			} catch (IOException e) {
				e.printStackTrace();
				throw new FuseException("access denied for " + path)
						.initErrno(FuseException.EACCES);
			} finally {
				
			}
		}
		return 0;
	}

	public int flush(String path, Object fh) throws FuseException {
		//SDFSLogger.getLog().info("1");
		DedupFileChannel ch = (DedupFileChannel) fh;
		try {
			ch.force(true);
		} catch (IOException e) {
			log.error("unable to sync file", e);
			throw new FuseException("symlink not supported")
					.initErrno(FuseException.ENOSYS);
		}
		return 0;
	}

	public int fsync(String path, Object fh, boolean isDatasync)
			throws FuseException {
		//SDFSLogger.getLog().info("2");
		DedupFileChannel ch = (DedupFileChannel) fh;
		try {
			ch.force(true);
		} catch (IOException e) {
			log.error("unable to sync file", e);
			throw new FuseException("unable to sync")
					.initErrno(FuseException.ENOSYS);
		}
		return 0;
	}

	public int getattr(String path, FuseGetattrSetter getattrSetter)
			throws FuseException {
		//SDFSLogger.getLog().info("5");
		int ftype = this.getFtype(path);
		if (ftype == FuseFtype.TYPE_SYMLINK) {
			Path p = null;
			BasicFileAttributes attrs = null;
			try {
				p = Paths.get(this.mountedVolume + path);
				int uid = 0;
				int gid = 0;
				int mode = 0000;
				try {
					uid = (Integer) p.getAttribute("unix:uid",
							LinkOption.NOFOLLOW_LINKS);
					gid = (Integer) p.getAttribute("unix:gid",
							LinkOption.NOFOLLOW_LINKS);
					mode = (Integer) p.getAttribute("unix:mode",
							LinkOption.NOFOLLOW_LINKS);
				} catch (Exception e) {
				}

				int atime = 0;
				int ctime = 0;
				int mtime = 0;
				long fileLength = 0;
				try {
					attrs = Attributes.readBasicFileAttributes(p,
							LinkOption.NOFOLLOW_LINKS);
					fileLength = attrs.size();
					mtime = (int) (attrs.lastModifiedTime().toMillis() / 1000L);
					atime = (int) (attrs.lastAccessTime().toMillis() / 1000L);
					ctime = (int) (attrs.creationTime().toMillis() / 1000L);

				} catch (Exception e) {
				}
				getattrSetter.set(p.hashCode(), mode, 1, uid, gid, 0,
						fileLength, (fileLength * NAME_LENGTH + BLOCK_SIZE - 1)
								/ BLOCK_SIZE, atime, mtime, ctime);
			} catch (Exception e) {
				log.error("unable to parse sylink " + path, e);
				throw new FuseException().initErrno(FuseException.EACCES);
			} finally {
				attrs = null;
				p = null;
			}
		} else {
			File f = resolvePath(path);
			Path p = null;
			try {
				p = Paths.get(f.getPath());
				if (f.isDirectory()) {
					int uid = (Integer) p.getAttribute("unix:uid");
					int gid = (Integer) p.getAttribute("unix:gid");
					int mode = (Integer) p.getAttribute("unix:mode");
					MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
					int atime = (int) (mf.getLastAccessed() / 1000L);
					int mtime = (int) (mf.lastModified() / 1000L);
					int ctime = (int) (mf.getTimeStamp() / 1000L);
					
					int fileLength = f.list().length;
					getattrSetter.set(mf.getGUID().hashCode(), mode, 1, uid,
							gid, 0, fileLength * NAME_LENGTH, (fileLength
									* NAME_LENGTH + BLOCK_SIZE - 1)
									/ BLOCK_SIZE, atime, mtime, ctime);
				} else {
					p = Paths.get(f.getPath());
					MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
					int uid = mf.getOwner_id();
					int gid = mf.getGroup_id();
					int mode = mf.getMode();
					int atime = (int) (mf.getLastAccessed() / 1000L);
					int ctime = (int) (mf.getTimeStamp() / 1000L);
					int mtime = (int) (mf.lastModified() / 1000L);
					long fileLength = mf.length();
					long actualBytes = (mf.getIOMonitor().getActualBytesWritten()*2)/1024;
					if(actualBytes == 0 && mf.getIOMonitor().getActualBytesWritten() >0)
						actualBytes = (Main.CHUNK_LENGTH*2)/1024;
					getattrSetter.set(mf.getGUID().hashCode(), mode, 1, uid,
							gid, 0, fileLength, actualBytes, atime, mtime, ctime);
				}
			} catch (Exception e) {
				log.error("unable to parse attributes " + path
						+ " at physical path " + f.getPath(), e);
				throw new FuseException().initErrno(FuseException.EACCES);
			} finally {
				f = null;
				p = null;
			}
		}
		return 0;
	}

	public int getdir(String path, FuseDirFiller dirFiller)
			throws FuseException {
		//SDFSLogger.getLog().info("6");
		File f = null;
		try {
			f = resolvePath(path);
			File[] mfs = f.listFiles();
			dirFiller.add(".", ".".hashCode(), FuseFtype.TYPE_DIR);
			dirFiller.add("..", "..".hashCode(), FuseFtype.TYPE_DIR);
			for (int i = 0; i < mfs.length; i++) {
				File _mf = mfs[i];
				dirFiller
						.add(_mf.getName(), _mf.hashCode(), this.getFtype(_mf));
			}
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			f = null;
		}
		return 0;
	}

	public int link(String from, String to) throws FuseException {
		throw new FuseException("error hard linking is not supported")
				.initErrno(FuseException.ENOSYS);
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

	public int mkdir(String path, int mode) throws FuseException {
		//SDFSLogger.getLog().info("7");
		File f = new File(this.mountedVolume + path);
		if(Main.volume.isFull())
			throw new FuseException("Volume Full")
		.initErrno(FuseException.ENOSPC);
		if (f.exists()) {
			f = null;
			throw new FuseException("folder exists")
					.initErrno(FuseException.EPERM);
		}
		f.mkdir();
		Path p = Paths.get(f.getPath());
		try {
			p.setAttribute("unix:mode", Integer.valueOf(mode));
		} catch (IOException e) {
			e.printStackTrace();
			throw new FuseException("access denied for " + path)
					.initErrno(FuseException.EACCES);
		} finally {
			path = null;
		}
		return 0;
	}

	public int mknod(String path, int mode, int rdev) throws FuseException {
		// log.info("mknod(): " + path + " " + mode + " " + rdev + "\n");
		//SDFSLogger.getLog().info("8");
		File f = new File(this.mountedVolume + path);
		if(Main.volume.isFull())
			throw new FuseException("Volume Full")
		.initErrno(FuseException.ENOSPC);
		if (f.exists()) {
			f = null;
			throw new FuseException("file exists")
					.initErrno(FuseException.EPERM);
		} else {

			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			mf.sync();
			try {
				mf.setMode(mode);
			} catch (IOException e) {
				e.printStackTrace();
				throw new FuseException("access denied for " + path)
						.initErrno(FuseException.EACCES);
			} finally {
				f = null;
			}

		}
		return 0;
	}

	public int open(String path, int flags, FuseOpenSetter openSetter)
			throws FuseException {
		//SDFSLogger.getLog().info("9");
		try {
			openSetter.setFh(this.getFileChannel(path));
		} catch (FuseException e) {
			e.printStackTrace();
			throw e;
		}
		return 0;
	}

	public int read(String path, Object fh, ByteBuffer buf, long offset)
			throws FuseException {
		//SDFSLogger.getLog().info("10");
		// log.info("Reading " + path + " at " + offset + " with buffer " +
		// buf.capacity());
		byte[] b = new byte[buf.capacity()];
		try {
			DedupFileChannel ch = (DedupFileChannel) fh;
			int read = ch.read(b, 0, b.length, offset);
			if (read == -1)
				read = 0;
			buf.put(b, 0, read);
		} catch (IOException e) {
			log.error("unable to read file " + path, e);
			throw new FuseException("error opening " + path)
					.initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int readlink(String path, CharBuffer link) throws FuseException {
		Path p = Paths.get(this.mountedVolume + path);
		//SDFSLogger.getLog().info("11");
		try {
			String lpath = p.readSymbolicLink().toString();
			link.put(lpath);
		} catch (IOException e) {
			e.printStackTrace();
			throw new FuseException("error getting linking " + path)
					.initErrno(FuseException.EACCES);
		} finally {
			p = null;
		}
		return 0;
	}

	public int release(String path, Object fh, int flags) throws FuseException {
		// log.info("closing " + path + " with flags " + flags);
		//SDFSLogger.getLog().info("12");
		DedupFileChannel ch = (DedupFileChannel) fh;
		try {
			ch.close();
		} catch (IOException e) {
			log.warn("unable to close " + path, e);
		}
		return 0;
	}

	public int rename(String from, String to) throws FuseException {
		//SDFSLogger.getLog().info("13");
		File f = null;
		try {
			f = resolvePath(from);

			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			mf.renameTo(this.mountedVolume + to);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FuseException().initErrno(FuseException.EACCES);
		} finally {
			f = null;
		}
		return 0;
	}

	public int rmdir(String path) throws FuseException {
		//SDFSLogger.getLog().info("14");
		if (this.getFtype(path) == FuseFtype.TYPE_SYMLINK) {
			File f = new File(mountedVolume + path);
			if (!f.delete()) {
				f = null;
				throw new FuseException().initErrno(FuseException.EACCES);
			}
			return 0;
		} else {
			File f = resolvePath(path);
			if (f.getName().equals(".") || f.getName().equals(".."))
				return 0;
			else {
				if (MetaFileStore.removeMetaFile(f.getPath()))
					return 0;
				else {
					log.debug("unable to delete folder " + f.getPath());
					throw new FuseException().initErrno(FuseException.ENOTEMPTY);
				}
			}
		}
	}

	public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
		// statfsSetter.set(blockSize, blocks, blocksFree, blocksAvail, files,
		// filesFree, namelen)
		//SDFSLogger.getLog().info("15");
		int blocks = (int) Main.volume.getTotalBlocks();

		int used = (int) Main.volume.getUsedBlocks();
		if (used > blocks)
			used = blocks;
		statfsSetter.set(Main.volume.getBlockSize(), blocks, blocks - used,
				blocks - used, (int) 0, 0, NAME_LENGTH);
		return 0;
	}

	public int symlink(String from, String to) throws FuseException {
		//SDFSLogger.getLog().info("16");
		File dst = new File(mountedVolume + to);
		if (dst.exists()) {
			throw new FuseException().initErrno(FuseException.EPERM);
		}
		Path srcP = Paths.get(from);
		Path dstP = Paths.get(dst.getPath());
		try {
			dstP.createSymbolicLink(srcP);
		} catch (IOException e) {

			log.error("error linking " + from + " to " + to, e);
			throw new FuseException().initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int truncate(String path, long size) throws FuseException {
		//SDFSLogger.getLog().info("17");
		try {
			DedupFileChannel ch = this.getFileChannel(path);
			ch.truncateFile(size);
			ch.close();
		} catch (IOException e) {
			log.error("unable to truncate file " + path, e);
			throw new FuseException().initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int unlink(String path) throws FuseException {
		//SDFSLogger.getLog().info("18");
		if (this.getFtype(path) == FuseFtype.TYPE_SYMLINK) {
			File f = new File(mountedVolume + path);
			if (!f.delete())
				throw new FuseException().initErrno(FuseException.EACCES);
			return 0;
		} else {
			File f = this.resolvePath(path);
			try {
				if (MetaFileStore.removeMetaFile(f.getPath()))
					return 0;
				else {
					log.warn("unable to delete folder " + f.getPath());
					throw new FuseException().initErrno(FuseException.ENOSYS);
				}
			} catch (Exception e) {
				log.error("unable to file file " + path, e);
				throw new FuseException().initErrno(FuseException.EACCES);
			}
		}
	}

	public int utime(String path, int atime, int mtime) throws FuseException {
		//SDFSLogger.getLog().info("19");
		File f = this.resolvePath(path);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		mf.setLastAccessed(atime * 1000L);
		mf.setLastModified(mtime * 1000L);
		return 0;
	}

	public int write(String path, Object fh, boolean isWritepage,
			ByteBuffer buf, long offset) throws FuseException {
		//SDFSLogger.getLog().info("19");
		if(Main.volume.isFull())
			throw new FuseException("Volume Full")
		.initErrno(FuseException.ENOSPC);
		/*
		 * log.info("writing data to  " +path + " at " + offset +
		 * " and length of " + buf.capacity());
		 */
		DedupFileChannel ch = (DedupFileChannel) fh;
		byte[] b = new byte[buf.capacity()];
		buf.get(b);
		try {
			ch.writeFile(b, b.length, 0, offset);
		} catch (IOException e) {
			log.error("unable to write to file" + path, e);
			throw new FuseException().initErrno(FuseException.EACCES);
		}
		return 0;
	}

	private File resolvePath(String path) throws FuseException {
		File _f = new File(mountedVolume + path);
		if (!_f.exists()) {
			_f = null;
			log.debug("No such node");
			throw new FuseException().initErrno(FuseException.ENOENT);
		}
		return _f;
	}

	private int getFtype(File _f) throws FuseException {
		Path p = Paths.get(_f.getPath());
		try {
			boolean isSymbolicLink = Attributes.readBasicFileAttributes(p,
					LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
			if (isSymbolicLink)
				return FuseFtype.TYPE_SYMLINK;
			else if (_f.isDirectory())
				return FuseFtype.TYPE_DIR;
			else if (_f.isFile())
				return FuseFtype.TYPE_FILE;
		} catch (IOException e) {
			e.printStackTrace();
			_f = null;
			p = null;
			throw new FuseException("No such node")
					.initErrno(FuseException.ENOENT);
		}
		throw new FuseException().initErrno(FuseException.ENOENT);
	}

	private int getFtype(String path) throws FuseException {
		File _f = new File(mountedVolume + path);
		if (!_f.exists()) {
			throw new FuseException().initErrno(FuseException.ENOENT);
		}
		Path p = Paths.get(_f.getPath());
		try {
			boolean isSymbolicLink = Attributes.readBasicFileAttributes(p,
					LinkOption.NOFOLLOW_LINKS).isSymbolicLink();
			if (isSymbolicLink)
				return FuseFtype.TYPE_SYMLINK;
			else if (_f.isDirectory())
				return FuseFtype.TYPE_DIR;
			else if (_f.isFile())
				return FuseFtype.TYPE_FILE;
		} catch (IOException e) {
			_f = null;
			p = null;
			e.printStackTrace();
			throw new FuseException("No such node")
					.initErrno(FuseException.ENOENT);
		}
		log.error("could not determine type for " + path);
		throw new FuseException().initErrno(FuseException.ENOENT);
	}

	private DedupFileChannel getFileChannel(String path) throws FuseException {
		File f = this.resolvePath(path);
		try {
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			return mf.getDedupFile().getChannel();
		} catch (IOException e) {
			log.error("unable to open file" + f.getPath(), e);
			throw new FuseException("error opening " + path)
					.initErrno(FuseException.EACCES);
		}
	}

	public int getxattr(String path, String name, ByteBuffer dst)
			throws FuseException, BufferOverflowException {
		int ftype = this.getFtype(path);
		if (ftype != FuseFtype.TYPE_SYMLINK) {
			if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs.")
					|| name.startsWith("user.dse"))
				dst.put(sdfsCmds.getAttr(name, path).getBytes());
			else {
				File f = this.resolvePath(path);

				MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
				String val = mf.getXAttribute(name);
				if (val != null) {
					log.debug("val=" + val);
					dst.put(val.getBytes());
				} else
					throw new FuseException().initErrno(FuseException.ENODATA);
			}
		}
		return 0;
	}

	public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter)
			throws FuseException {
		//SDFSLogger.getLog().info("21");
		if (name.startsWith("security.capability"))
			return 0;
		if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs")
				|| name.startsWith("user.dse")) {
			try {
				sizeSetter
						.setSize(sdfsCmds.getAttr(name, path).getBytes().length);
			} catch (Exception e) {
				log.error("attribure get error for " + name, e);
			}
		} else {
			File f = this.resolvePath(path);
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			String val = mf.getXAttribute(name);
			if (val != null)
				sizeSetter.setSize(val.getBytes().length);
		}
		return 0;
	}

	public int listxattr(String path, XattrLister lister) throws FuseException {
		sdfsCmds.listAttrs(lister);
		return 0;
	}

	public int removexattr(String path, String name) throws FuseException {
		return 0;
	}

	public int setxattr(String path, String name, ByteBuffer value, int flags)
			throws FuseException {
		byte valB[] = new byte[value.capacity()];
		value.get(valB);
		String valStr = new String(valB);
		if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs.")
				|| name.startsWith("user.dse")) {
			sdfsCmds.runCMD(path, name, valStr);
		} else {
			File f = this.resolvePath(path);
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			mf.addXAttribute(name, valStr);
		}
		return 0;
	}

}
