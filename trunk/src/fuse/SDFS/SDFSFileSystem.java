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
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.servers.SDFSService;

import com.sun.org.apache.bcel.internal.generic.RETURN;

import fuse.Filesystem3;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseFtype;
import fuse.FuseFtypeConstants;
import fuse.FuseGetattrSetter;
import fuse.FuseMount;
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
	static long tbc = 1024 * 1024 * 1024 * 1024;
	static int gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;
	static int kbc = 1024;
	private SDFSCmds sdfsCmds;

	private HashMap<String, DedupFileChannel> channels = new HashMap<String, DedupFileChannel>();

	
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
		File f = resolvePath(path);
		Path p = Paths.get(f.getPath());
		try {
			p.setAttribute("unix:mode", Integer.valueOf(mode));
		} catch (IOException e) {
			throw new FuseException("access denied for " + path)
			.initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int chown(String path, int uid, int gid) throws FuseException {
		File f = resolvePath(path);
		Path p = Paths.get(f.getPath());
		try {
			p.setAttribute("unix:uid", Integer.valueOf(uid));
			p.setAttribute("unix:gid", Integer.valueOf(gid));
		} catch (IOException e) {
			throw new FuseException("access denied for " + path)
			.initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int flush(String path, Object fh) throws FuseException {
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
		int ftype =this.getFtype(path);
		if (ftype == FuseFtype.TYPE_SYMLINK) {
			try {
			Path p = Paths.get(this.mountedVolume + path);
			int uid = (Integer)p.getAttribute("unix:uid");
			int gid = (Integer)p.getAttribute("unix:gid");
			BasicFileAttributes attrs = Attributes.readBasicFileAttributes(p);
			int atime = (int) (attrs.lastAccessTime().toMillis() / 1000L);
			int mtime = (int) (attrs.lastModifiedTime().toMillis() / 1000L);
			getattrSetter.set(p.hashCode(), FuseFtype.TYPE_SYMLINK | 0777, 1, uid, gid, 0,
					8, (NAME_LENGTH + BLOCK_SIZE - 1)
							/ BLOCK_SIZE, atime, mtime, 0);
			}catch(Exception e) {
				log.error("unable to parse sylink " + path,e);
				throw new FuseException("error getting symlink " + path)
				.initErrno(FuseException.EACCES);
			}
		} else {
			File f = resolvePath(path);
			try {
			Path p = Paths.get(f.getPath());
			int uid = (Integer)p.getAttribute("unix:uid");
			int gid = (Integer)p.getAttribute("unix:gid");
			int mode = (Integer)p.getAttribute("unix:mode");
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			int atime = (int) (mf.lastModified() / 1000L);
			int ctime = (int) (mf.getTimeStamp() / 1000L);
			if (mf.isDirectory()) {
				int fileLength = mf.list().length;
				getattrSetter.set(mf.getGUID().hashCode(), this.getFtype(path)
						|mode, 1, uid, gid, 0, fileLength * NAME_LENGTH,
						(fileLength * NAME_LENGTH + BLOCK_SIZE - 1)
								/ BLOCK_SIZE, atime, atime, ctime);
			} else {
				long fileLength = mf.length();
				getattrSetter.set(mf.getGUID().hashCode(), this.getFtype(path)
						| mode, 1, uid, gid, 0, fileLength,
						(fileLength + BLOCK_SIZE - 1) / BLOCK_SIZE, atime,
						atime, ctime);
			}
			}catch(Exception e) {
				log.error("unable to parse sylink " + path,e);
				throw new FuseException("error getting symlink " + path)
				.initErrno(FuseException.EACCES);
			}
		}
		return 0;
	}

	public int getdir(String path, FuseDirFiller dirFiller)
			throws FuseException {
		File f = resolvePath(path);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		MetaDataDedupFile[] mfs = mf.listFiles();
		for (int i = 0; i < mfs.length; i++) {
			MetaDataDedupFile _mf = mfs[i];
			dirFiller.add(_mf.getName(), _mf.hashCode(), this.getFtype(path));
		}
		return 0;
	}

	public int link(String from, String to) throws FuseException {
		log.debug("link(): " + from + " to " + to);
		File dst = new File(mountedVolume + to);
		if (dst.exists()) {
			throw new FuseException("file exists")
					.initErrno(FuseException.EPERM);
		}
		Path srcP = Paths.get(from);
		Path dstP = Paths.get(dst.getPath());
		try {
			dstP.createLink(srcP);
		} catch (IOException e) {
			log.error("error linking " + from + " to " + to, e);
			throw new FuseException("error linking " + from + " to " + to)
					.initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int mkdir(String path, int mode) throws FuseException {
		MetaDataDedupFile mf = MetaFileStore.getMF(mountedVolume + path);
		mf.mkdir();
		return 0;
	}

	public int mknod(String path, int mode, int rdev) throws FuseException {
		// log.info("mknod(): " + path + " " + mode + " " + rdev + "\n");
		File f = new File(this.mountedVolume + path);
		if (f.exists())
			throw new FuseException("file exists")
					.initErrno(FuseException.EPERM);
		else {
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			mf.sync();
			Path p = Paths.get(f.getPath());
			try {
				p.setAttribute("unix:mode", Integer.valueOf(mode));
			} catch (IOException e) {
				throw new FuseException("access denied for " + path)
				.initErrno(FuseException.EACCES);
			}
			
		}
		return 0;
	}

	public int open(String path, int flags, FuseOpenSetter openSetter)
			throws FuseException {
		// log.debug("opening "+path+" with flags "+ flags +" and openSetter " +
		// openSetter.isDirectIO() + openSetter.isKeepCache());
		openSetter.setFh(this.getFileChannel(path));
		return 0;
	}

	public int read(String path, Object fh, ByteBuffer buf, long offset)
			throws FuseException {
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
		log.info("reading link for " + path);
		Path p = Paths.get(this.mountedVolume + path);
		try {
			String lpath = p.readSymbolicLink().toString();
			log.info("linked path is " + lpath);
			link.put(lpath);
		} catch (IOException e) {
			throw new FuseException("error getting linking " + path)
					.initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int release(String path, Object fh, int flags) throws FuseException {
		// log.info("closing " + path + " with flags " + flags);
		DedupFileChannel ch = (DedupFileChannel) fh;
		try {
			ch.close();
		} catch (IOException e) {
			log.warn("unable to close " + path, e);
		}
		return 0;
	}

	public int rename(String from, String to) throws FuseException {
		File f = resolvePath(from);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		MetaDataDedupFile newMF = MetaFileStore.getMF(this.mountedVolume + to);
		mf.renameTo(newMF);
		return 0;
	}

	public int rmdir(String path) throws FuseException {
		if (this.getFtype(path) == FuseFtype.TYPE_SYMLINK) {
			File f = new File(mountedVolume + path);
			if (!f.delete())
				throw new FuseException("error deleting symlink " + path)
						.initErrno(FuseException.EACCES);
			return 0;
		} else {
			File f = resolvePath(path);
			if (f.getName().equals(".") || f.getName().equals(".."))
				return 0;
			else {
				if (MetaFileStore.removeMetaFile(f.getPath()))
					return 0;
				else {
					log.warn("unable to delete folder " + f.getPath());
					throw new FuseException("unable to delete folder")
							.initErrno(FuseException.ENOSYS);
				}
			}
		}
	}

	public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
		// statfsSetter.set(blockSize, blocks, blocksFree, blocksAvail, files,
		// filesFree, namelen)
		int blocks = (int) Main.volume.getTotalBlocks();

		int used = (int) Main.volume.getUsedBlocks();
		if (used > blocks)
			used = blocks;
		statfsSetter
				.set(Main.volume.getBlockSize(), blocks, blocks - used, blocks
						- used, (int) MetaFileStore.getEntries(), 0,
						NAME_LENGTH);
		return 0;
	}

	public int symlink(String from, String to) throws FuseException {
		log.info("symlink(): " + from + " to " + to);
		File dst = new File(mountedVolume + to);
		if (dst.exists()) {
			throw new FuseException("file exists")
					.initErrno(FuseException.EPERM);
		}
		Path srcP = Paths.get(from);
		Path dstP = Paths.get(dst.getPath());
		try {
			dstP.createSymbolicLink(srcP);
		} catch (IOException e) {
			log.error("error linking " + from + " to " + to, e);
			throw new FuseException("error linking " + from + " to " + to)
					.initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int truncate(String path, long size) throws FuseException {
		try {
			DedupFileChannel ch = this.getFileChannel(path);
			ch.truncateFile(size);
			ch.close();
		} catch (IOException e) {
			log.error("unable to truncate file " + path, e);
			throw new FuseException("error truncating " + path)
					.initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int unlink(String path) throws FuseException {
		if (this.getFtype(path) == FuseFtype.TYPE_SYMLINK) {
			File f = new File(mountedVolume + path);
			if (!f.delete())
				throw new FuseException("error deleting symlink " + path)
						.initErrno(FuseException.EACCES);
			return 0;
		} else {
			File f = this.resolvePath(path);
			try {
				if (MetaFileStore.removeMetaFile(f.getPath()))
					return 0;
				else {
					log.warn("unable to delete folder " + f.getPath());
					throw new FuseException("unable to delete folder")
							.initErrno(FuseException.ENOSYS);
				}
			} catch (Exception e) {
				log.error("unable to file file " + path, e);
				throw new FuseException("error deleting " + path)
						.initErrno(FuseException.EACCES);
			}
		}
	}

	public int utime(String path, int atime, int mtime) throws FuseException {
		File f = this.resolvePath(path);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		try {
			mf.setLastAccessed(atime * 1000L);
		
		mf.setLastModified(mtime * 1000L);
		} catch (IOException e) {
			log.error("unable to change utime" + path, e);
			throw new FuseException("error changing utime for " + path)
					.initErrno(FuseException.EACCES);
		}
		return 0;
	}

	public int write(String path, Object fh, boolean isWritepage,
			ByteBuffer buf, long offset) throws FuseException {
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
			throw new FuseException("error writing to " + path)
					.initErrno(FuseException.EACCES);
		}
		return 0;
	}
	

	private File resolvePath(String path) throws FuseException {
		File _f = new File(mountedVolume + path);
		if (!_f.exists()) {
			_f = null;
			throw new FuseException("No such node")
					.initErrno(FuseException.ENOENT);
		}
		return _f;

	}

	
	private int getFtype(String path) throws FuseException {
		File _f = new File(mountedVolume + path);
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
			throw new FuseException("No such node")
					.initErrno(FuseException.ENOENT);
		}
		throw new FuseException("could not determine type")
				.initErrno(FuseException.ENOENT);

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
		if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs."))
			dst.put(sdfsCmds.getAttr(name, path).getBytes());

		return 0;
	}

	public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter)
			throws FuseException {
		if (name.startsWith("security.capability"))
			return 0;
		if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs")) {
			try {
				sizeSetter
						.setSize(sdfsCmds.getAttr(name, path).getBytes().length);
			} catch (Exception e) {
				log.error("attribure get error for " + name, e);
			}
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
		if (name.startsWith("user.cmd.") || name.startsWith("user.sdfs.")) {
			sdfsCmds.runCMD(path, name, valStr);
		}
		return 0;
	}

}
