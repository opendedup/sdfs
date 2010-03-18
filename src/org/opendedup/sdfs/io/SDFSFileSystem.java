package org.opendedup.sdfs.io;

import java.io.File;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;

public class SDFSFileSystem {

	public String mountedVolume;
	public String mountPoint;
	private static final Log log = LogFactory.getLog(SDFSFileSystem.class);
	static long tbc = 1024 * 1024 * 1024 * 1024;
	static int gbc = 1024 * 1024 * 1024;
	static int mbc = 1024 * 1024;
	static int kbc = 1024;

	private HashMap<String, DedupFileChannel> channels = new HashMap<String, DedupFileChannel>();

	public SDFSFileSystem(String mountedVolume, String mountPoint) {

		log.info("mounting " + mountedVolume + " to " + mountPoint);
		this.mountedVolume = mountedVolume;
		this.mountPoint = mountPoint;
		File f = new File(this.mountedVolume);
		if (!f.exists())
			f.mkdirs();
	}

	public int chmod(String path, int mode) throws IOException {
		File f = resolvePath(path);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		mf.setPermissions(mode);
		mf.sync();
		return 0;
	}

	public int chown(String path, int uid, int gid) throws IOException {
		File f = resolvePath(path);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		if (gid >= 0)
			mf.setGroup_id(gid);
		if (uid >= 0)
			mf.setOwner_id(uid);
		mf.sync();
		return 0;
	}

	public int flush(String path) throws IOException {
		DedupFileChannel ch = this.getFileChannel(path);
		try {
			ch.force(true);
		} catch (IOException e) {
			log.error("unable to sync file", e);
			throw new IOException("symlink not supported");
		}
		return 0;
	}

	public int fsync(String path, boolean isDatasync)
			throws IOException {
		DedupFileChannel ch = this.getFileChannel(path);
		try {
			ch.force(true);
		} catch (IOException e) {
			log.error("unable to sync file", e);
			throw new IOException("symlink not supported");
		}
		return 0;
	}

	public MetaDataDedupFile getdir(String path) throws IOException {
		File f = resolvePath(path);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());

		return mf;
	}

	public int link(String from, String to) throws IOException {
		log.info("symlink(): " + from + "\n");
		throw new IOException("symlink not supported");
	}

	public int mkdir(String path, int mode) throws IOException {
		MetaDataDedupFile mf = MetaFileStore.getMF(mountedVolume + path);
		mf.mkdir();
		return 0;
	}

	public int mknod(String path, int mode, int rdev) throws IOException {
		// log.info("mknod(): " + path + " " + mode + " " + rdev + "\n");
		File f = new File(this.mountedVolume + path);
		if (f.exists())
			throw new IOException("file exists");
		else {
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			mf.sync();
		}
		return 0;
	}

	public DedupFileChannel open(String path) throws IOException {

		return this.getFileChannel(path);
	}

	public int read(String path,ByteBuffer buf, long offset)
			throws IOException {
		// log.info("Reading " + path + " at " + offset + " with buffer " +
		// buf.capacity());
		byte[] b = new byte[buf.capacity()];
		try {
			DedupFileChannel ch = this.getFileChannel(path);
			int read = ch.read(b, 0, b.length, offset);
			if (read == -1)
				read = 0;
			buf.put(b, 0, read);
		} catch (IOException e) {
			log.error("unable to read file " + path, e);
			throw new IOException("error opening " + path);
		}
		return 0;
	}

	public int readlink(String path, CharBuffer link) throws IOException {
		throw new IOException("readLink not supported");
		// return 0;
	}

	public void release(String path, int flags) throws IOException {
		// log.info("closing " + path + " with flags " + flags);
		DedupFileChannel ch = this.channels.get(path);
		if (ch != null) {
			try {
				ch.close();
				this.channels.remove(path);
			} catch (IOException e) {
				log.warn("unable to close " + path, e);
			}
		}
	}

	public int rename(String from, String to) throws IOException {
		File f = resolvePath(from);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		MetaDataDedupFile newMF = MetaFileStore.getMF(this.mountedVolume + to);
		mf.renameTo(newMF);
		return 0;
	}

	public int rmdir(String path) throws IOException {
		File f = resolvePath(path);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		if (mf.delete())
			return 0;
		else {
			log.warn("unable to delete folder " + f.getPath());
			throw new IOException("unable to delete folder");
		}
	}

	public Volume statfs() {

		return Main.volume;
	}

	public int symlink(String from, String to) throws IOException {
		log.info("Symlink is not supported");
		throw new IOException("symlink not supported");
		// return 0;
	}

	public int truncate(String path, long size) throws IOException {
		try {
			DedupFileChannel ch = this.getFileChannel(path);
			ch.truncateFile(size);
			ch.close();
		} catch (IOException e) {
			log.error("unable to truncate file " + path, e);
			throw new IOException("error truncating " + path);
		}
		return 0;
	}

	public int unlink(String path) throws IOException {
		File f = this.resolvePath(path);
		try {
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			mf.delete();
		} catch (Exception e) {
			log.error("unable to file file " + path, e);
			throw new IOException("error deleting " + path);
		}
		return 0;
	}

	public int utime(String path, int atime, int mtime) throws IOException {
		// not supported right now (write-once files...)
		return 0;
	}

	public int write(String path, boolean isWritepage, ByteBuffer buf,
			long offset) throws IOException {
		/*
		 * log.info("writing data to  " +path + " at " + offset +
		 * " and length of " + buf.capacity());
		 */
		DedupFileChannel ch = this.getFileChannel(path);
		byte[] b = new byte[buf.capacity()];
		buf.get(b);
		try {
			ch.writeFile(b, b.length, 0, offset);
		} catch (IOException e) {
			log.error("unable to write to file" + path, e);
			throw new IOException("error writing to " + path);
		}
		return 0;
	}

	private File resolvePath(String path) throws IOException {
		File f = new File(mountedVolume + path);
		if (!f.exists()) {
			throw new IOException("No such node");

		}
		return f;
	}

	private DedupFileChannel getFileChannel(String path) throws IOException {
		File f = this.resolvePath(path);
		DedupFileChannel ch = this.channels.get(path);
		if (ch == null) {
			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
				ch = mf.getDedupFile().getChannel();
				this.channels.put(path, ch);
			} catch (IOException e) {
				log.error("unable to open file" + f.getPath(), e);
				throw new IOException("error opening " + path);
			}
		}
		return ch;
	}

}
