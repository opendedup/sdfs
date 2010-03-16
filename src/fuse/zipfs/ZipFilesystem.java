/**
 *   FUSE-J: Java bindings for FUSE (Filesystem in Userspace by Miklos Szeredi (mszeredi@inf.bme.hu))
 *
 *   Copyright (C) 2003 Peter Levart (peter@select-tech.si)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 */

package fuse.zipfs;

import fuse.*;
import fuse.compat.Filesystem1;
import fuse.compat.FuseDirEnt;
import fuse.compat.FuseStat;
import fuse.zipfs.util.Node;
import fuse.zipfs.util.Tree;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class ZipFilesystem implements Filesystem1 {
	private static final Log log = LogFactory.getLog(ZipFilesystem.class);

	private static final int blockSize = 512;

	private ZipFile zipFile;
	private long zipFileTime;
	private ZipEntry rootEntry;
	private Tree tree;
	private FuseStatfs statfs;

	private ZipFileDataReader zipFileDataReader;

	public ZipFilesystem(File file) throws IOException {
		log.info("extracting zip file structure...");
		zipFile = new ZipFile(file, ZipFile.OPEN_READ);
		zipFileTime = file.lastModified();
		rootEntry = new ZipEntry("") {
			public boolean isDirectory() {
				return true;
			}
		};
		rootEntry.setTime(zipFileTime);
		rootEntry.setSize(0);

		zipFileDataReader = new ZipFileDataReader(zipFile);

		tree = new Tree();
		tree.addNode(rootEntry.getName(), rootEntry);

		int files = 0;
		int dirs = 0;
		int blocks = 0;

		for (Enumeration<?> e = zipFile.entries(); e.hasMoreElements();) {
			ZipEntry entry = (ZipEntry) e.nextElement();
			tree.addNode(entry.getName(), entry);

			if (entry.isDirectory())
				dirs++;
			else
				files++;

			blocks += (entry.getSize() + blockSize - 1) / blockSize;
		}

		statfs = new FuseStatfs();
		statfs.blocks = blocks;
		statfs.blockSize = blockSize;
		statfs.blocksFree = 0;
		statfs.files = files + dirs;
		statfs.filesFree = 0;
		statfs.namelen = 2048;

		log.info("zip file structure extracted: " + files + " files, " + dirs
				+ " directories, " + blocks + " blocks (" + blockSize
				+ " byte/block).");
	}

	public void chmod(String path, int mode) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public void chown(String path, int uid, int gid) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public FuseStat getattr(String path) throws FuseException {
		Node node = tree.lookupNode(path);
		ZipEntry entry = null;
		if (node == null || (entry = (ZipEntry) node.getValue()) == null)
			throw new FuseException("No Such Entry")
					.initErrno(FuseException.ENOENT);

		FuseStat stat = new FuseStat();

		stat.mode = entry.isDirectory() ? FuseFtype.TYPE_DIR | 0755
				: FuseFtype.TYPE_FILE | 0644;
		stat.nlink = 1;
		stat.uid = 0;
		stat.gid = 0;
		stat.size = entry.getSize();
		stat.atime = stat.mtime = stat.ctime = (int) (entry.getTime() / 1000L);
		stat.blocks = (int) ((stat.size + 511L) / 512L);

		return stat;
	}

	public FuseDirEnt[] getdir(String path) throws FuseException {
		Node node = tree.lookupNode(path);
		ZipEntry entry = null;
		if (node == null || (entry = (ZipEntry) node.getValue()) == null)
			throw new FuseException("No Such Entry")
					.initErrno(FuseException.ENOENT);

		if (!entry.isDirectory())
			throw new FuseException("Not A Directory")
					.initErrno(FuseException.ENOTDIR);

		Collection<?> children = node.getChildren();
		FuseDirEnt[] dirEntries = new FuseDirEnt[children.size()];

		int i = 0;
		for (Iterator<?> iter = children.iterator(); iter.hasNext(); i++) {
			Node childNode = (Node) iter.next();
			ZipEntry zipEntry = (ZipEntry) childNode.getValue();
			FuseDirEnt dirEntry = new FuseDirEnt();
			dirEntries[i] = dirEntry;
			dirEntry.name = childNode.getName();
			dirEntry.mode = zipEntry.isDirectory() ? FuseFtype.TYPE_DIR
					: FuseFtype.TYPE_FILE;
		}

		return dirEntries;
	}

	public void link(String from, String to) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public void mkdir(String path, int mode) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public void mknod(String path, int mode, int rdev) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public void open(String path, int flags) throws FuseException {
		ZipEntry entry = getFileZipEntry(path);

		if (flags == O_WRONLY || flags == O_RDWR)
			throw new FuseException("Read Only")
					.initErrno(FuseException.EACCES);
	}

	public void rename(String from, String to) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public void rmdir(String path) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public FuseStatfs statfs() throws FuseException {
		return statfs;
	}

	public void symlink(String from, String to) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public void truncate(String path, long size) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public void unlink(String path) throws FuseException {
		throw new FuseException("Read Only").initErrno(FuseException.EACCES);
	}

	public void utime(String path, int atime, int mtime) throws FuseException {
		// noop
	}

	public String readlink(String path) throws FuseException {
		throw new FuseException("Not a link").initErrno(FuseException.ENOENT);
	}

	public void write(String path, ByteBuffer buf, long offset)
			throws FuseException {
		// noop
	}

	public void read(String path, ByteBuffer buf, long offset)
			throws FuseException {
		ZipEntry zipEntry = getFileZipEntry(path);
		ZipEntryDataReader reader = zipFileDataReader.getZipEntryDataReader(
				zipEntry, offset, buf.capacity());

		reader.read(buf, offset);
	}

	public void release(String path, int flags) throws FuseException {
		ZipEntry zipEntry = getFileZipEntry(path);
		zipFileDataReader.releaseZipEntryDataReader(zipEntry);
	}

	//
	// private methods

	private ZipEntry getFileZipEntry(String path) throws FuseException {
		Node node = tree.lookupNode(path);
		ZipEntry entry;
		if (node == null || (entry = (ZipEntry) node.getValue()) == null)
			throw new FuseException("No Such Entry")
					.initErrno(FuseException.ENOENT);

		if (entry.isDirectory())
			throw new FuseException("Not A File")
					.initErrno(FuseException.ENOENT);

		return entry;
	}

	private ZipEntry getDirectoryZipEntry(String path) throws FuseException {
		Node node = tree.lookupNode(path);
		ZipEntry entry;
		if (node == null || (entry = (ZipEntry) node.getValue()) == null)
			throw new FuseException("No Such Entry")
					.initErrno(FuseException.ENOENT);

		if (!entry.isDirectory())
			throw new FuseException("Not A Directory")
					.initErrno(FuseException.ENOENT);

		return entry;
	}

	//
	// Java entry point

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Must specify ZIP file");
			System.exit(-1);
		}

		String fuseArgs[] = new String[args.length - 1];
		System.arraycopy(args, 0, fuseArgs, 0, fuseArgs.length);
		File zipFile = new File(args[args.length - 1]);

		try {
			FuseMount.mount(fuseArgs, new ZipFilesystem(zipFile));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
