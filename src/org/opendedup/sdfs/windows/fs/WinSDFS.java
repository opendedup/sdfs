/*
The MIT License

Copyright (C) 2008 Yu Kobayashi http://yukoba.accelart.jp/

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 */

package org.opendedup.sdfs.windows.fs;

import static net.decasdev.dokan.CreationDisposition.CREATE_ALWAYS;

import static net.decasdev.dokan.CreationDisposition.CREATE_NEW;
import static net.decasdev.dokan.CreationDisposition.OPEN_ALWAYS;
import static net.decasdev.dokan.CreationDisposition.OPEN_EXISTING;
import static net.decasdev.dokan.CreationDisposition.TRUNCATE_EXISTING;
import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_DIRECTORY;
import static net.decasdev.dokan.FileAttribute.FILE_ATTRIBUTE_NORMAL;
import static net.decasdev.dokan.WinError.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.SDFSLogger;

import fuse.FuseException;
import fuse.FuseFtype;

import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.Dokan;
import net.decasdev.dokan.DokanDiskFreeSpace;
import net.decasdev.dokan.DokanFileInfo;
import net.decasdev.dokan.DokanOperationException;
import net.decasdev.dokan.DokanOperations;
import net.decasdev.dokan.DokanOptions;
import net.decasdev.dokan.DokanVolumeInformation;
import net.decasdev.dokan.FileTimeUtils;
import net.decasdev.dokan.Win32FindData;
import net.decasdev.dokan.WinError;

public class WinSDFS implements DokanOperations {
	/** fileName -> MemFileInfo */
	// TODO FIX THIS
	final static int volumeSerialNumber = 6442;
	/** Next handle */
	long nextHandleNo = 1;
	final long rootCreateTime = FileTimeUtils.toFileTime(new Date());
	long rootLastWrite = rootCreateTime;
	private String mountedVolume = null;
	private char driveLetter = 'S';
	private Log log = SDFSLogger.getLog();

	static void log(String msg) {
		System.out.println("== app == " + msg);
	}

	public WinSDFS() {
		showVersions();
	}

	void showVersions() {
		int version = Dokan.getVersion();
		System.out.println("version = " + version);
		int driverVersion = Dokan.getDriverVersion();
		System.out.println("driverVersion = " + driverVersion);
	}

	void mount(char driveLetter,String mountedVolume) {
		this.mountedVolume = mountedVolume;
		DokanOptions dokanOptions = new DokanOptions();
		dokanOptions.driveLetter = driveLetter;
		this.driveLetter = driveLetter;
		int result = Dokan.mount(dokanOptions, this);
		log("[MemoryFS] result = " + result);
		log.info("mounted " + mountedVolume + " to " + this.driveLetter);
	}

	synchronized long getNextHandle() {
		return nextHandleNo++;
	}

	public long onCreateFile(String fileName, int desiredAccess, int shareMode,
			int creationDisposition, int flagsAndAttributes, DokanFileInfo arg5)
			throws DokanOperationException {
		log("[onCreateFile] " + fileName + ", creationDisposition = "
				+ creationDisposition);
		if (fileName.equals("\\")) {
			switch (creationDisposition) {
			case CREATE_NEW:
			case CREATE_ALWAYS:
				throw new DokanOperationException(ERROR_ALREADY_EXISTS);
			case OPEN_ALWAYS:
			case OPEN_EXISTING:
			case TRUNCATE_EXISTING:
				return getNextHandle();
			}
		} else if (this.resolvePath(fileName).exists()) {
			switch (creationDisposition) {
			case CREATE_NEW:
				throw new DokanOperationException(ERROR_ALREADY_EXISTS);
			case OPEN_ALWAYS:
			case OPEN_EXISTING:
				return getNextHandle();
			case CREATE_ALWAYS:
			case TRUNCATE_EXISTING:
				try {
					this.getFileChannel(fileName).truncateFile(0);
					return getNextHandle();
				} catch (IOException e) {
					log.error(
							"unable to clear file "
									+ this.resolvePath(fileName).getPath(), e);
					throw new DokanOperationException(
							WinError.ERROR_WRITE_FAULT);
				}
			}
		} else {
			switch (creationDisposition) {
			case CREATE_NEW:
			case CREATE_ALWAYS:
			case OPEN_ALWAYS:
				try {
					MetaDataDedupFile mf = MetaFileStore.getMF(this
							.resolvePath(fileName).getPath());
					mf.sync();
				} catch (Exception e) {
					log.error(
							"unable to create file "
									+ this.resolvePath(fileName).getPath(), e);
					throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
				}
				return getNextHandle();
			case OPEN_EXISTING:
			case TRUNCATE_EXISTING:
				throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
			}
		}
		throw new DokanOperationException(1);
	}

	public long onOpenDirectory(String pathName, DokanFileInfo arg1)
			throws DokanOperationException {
		log("[onOpenDirectory] " + pathName);
		if (pathName.equals("\\"))
			return getNextHandle();
		pathName = Utils.trimTailBackSlash(pathName);
		File _f = new File(mountedVolume + pathName);
		if (_f.exists())
			return getNextHandle();
		else
			throw new DokanOperationException(ERROR_PATH_NOT_FOUND);
	}

	public void onCreateDirectory(String pathName, DokanFileInfo file)
			throws DokanOperationException {
		log("[onCreateDirectory] " + pathName);
		pathName = Utils.trimTailBackSlash(pathName);
		File f = new File(this.mountedVolume + pathName);
		if (f.exists()) {
			f = null;
			throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
		}
		f.mkdir();
	}

	public void onCleanup(String arg0, DokanFileInfo arg2)
			throws DokanOperationException {
	}

	public void onCloseFile(String path, DokanFileInfo arg1)
			throws DokanOperationException {

		DedupFileChannel ch = this.getFileChannel(path);
		try {
			ch.close();
		} catch (IOException e) {
			log.error("unable to close " + ch.getFile().getPath());
		}
	}

	public int onReadFile(String fileName, ByteBuffer buf, long offset,
			DokanFileInfo arg3) throws DokanOperationException {
		log("[onReadFile] " + fileName);
		DedupFileChannel ch = this.getFileChannel(fileName);
		byte[] b = new byte[buf.capacity()];
		try {
			int read = ch.read(b, 0, b.length, offset);
			if (read == -1)
				read = 0;
			buf.put(b, 0, read);
			return read;
		} catch (IOException e) {
			log.error("unable to read file " + fileName, e);
			throw new DokanOperationException(ERROR_READ_FAULT);
		}
	}

	public int onWriteFile(String fileName, ByteBuffer buf, long offset,
			DokanFileInfo arg3) throws DokanOperationException {
		log("[onWriteFile] " + fileName);
		DedupFileChannel ch = this.getFileChannel(fileName);
		byte[] b = new byte[buf.capacity()];
		buf.get(b);
		try {
			ch.writeFile(b, b.length, 0, offset);
		} catch (IOException e) {
			log.error("unable to write to file" + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
		return b.length;
	}

	public void onSetEndOfFile(String fileName, long length, DokanFileInfo arg2)
			throws DokanOperationException {
		log("[onSetEndOfFile] " + fileName);
		DedupFileChannel ch = this.getFileChannel(fileName);
		try {
			ch.truncateFile(length);
		} catch (IOException e) {
			log.error("Unable to set length  of " + fileName + " to " + length);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}

	}

	public void onFlushFileBuffers(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {
		DedupFileChannel ch = this.getFileChannel(fileName);
		try {
			ch.force(true);
		} catch (IOException e) {
			log.error("unable to sync file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}

	}

	public ByHandleFileInformation onGetFileInformation(String fileName,
			DokanFileInfo arg1) throws DokanOperationException {
		log("[onGetFileInformation] " + fileName);
		if (fileName.equals("\\")) {
			return new ByHandleFileInformation(FILE_ATTRIBUTE_NORMAL
					| FILE_ATTRIBUTE_DIRECTORY, rootCreateTime, rootCreateTime,
					rootLastWrite, volumeSerialNumber, 0, 1, 1);
		}
		MetaDataDedupFile mf = MetaFileStore.getMF(this.resolvePath(fileName)
				.getPath());
		MetaDataFileInfo fi = new MetaDataFileInfo(fileName, mf);
		return fi.toByHandleFileInformation();
	}

	public Win32FindData[] onFindFiles(String pathName, DokanFileInfo arg1)
			throws DokanOperationException {
		log("[onFindFiles] " + pathName);
		File f = null;
		try {
			f = resolvePath(pathName);
			File[] mfs = f.listFiles();
			ArrayList<Win32FindData> files = new ArrayList<Win32FindData>();
			for (int i = 0; i < mfs.length; i++) {
				File _mf = mfs[i];
				MetaDataDedupFile mf = MetaFileStore.getMF(_mf.getPath());
				MetaDataFileInfo fi = new MetaDataFileInfo(_mf.getName(), mf);
				files.add(fi.toWin32FindData());
			}
			log("[onFindFiles] " + files);
			return files.toArray(new Win32FindData[0]);
		} catch (Exception e) {
			log.error("unable to list files for " + pathName, e);
			throw new DokanOperationException(WinError.ERROR_DIRECTORY);
		} finally {
			f = null;
		}

	}

	public Win32FindData[] onFindFilesWithPattern(String arg0, String arg1,
			DokanFileInfo arg2) throws DokanOperationException {
		return null;
	}

	public void onSetFileAttributes(String fileName, int fileAttributes,
			DokanFileInfo arg2) throws DokanOperationException {
		log("[onSetFileAttributes] " + fileName);
		/*
		 * MemFileInfo fi = fileInfoMap.get(fileName); if (fi == null) throw new
		 * DokanOperationException(ERROR_FILE_NOT_FOUND); fi.fileAttribute =
		 * fileAttributes;
		 */
	}

	public void onSetFileTime(String fileName, long creationTime, long atime,
			long mtime, DokanFileInfo arg4) throws DokanOperationException {
		log("[onSetFileTime] " + fileName);
		File f = this.resolvePath(fileName);
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		mf.setLastAccessed(atime * 1000L);
		mf.setLastModified(mtime * 1000L);
	}

	public void onDeleteFile(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {
		log("[onDeleteFile] " + fileName);
		File f = this.resolvePath(fileName);
		try {
			if (!MetaFileStore.removeMetaFile(f.getPath()))
				log.warn("unable to delete folder " + f.getPath());
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);

		} catch (Exception e) {
			log.error("unable to file file " + fileName, e);
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		}
	}

	public void onDeleteDirectory(String path, DokanFileInfo arg1)
			throws DokanOperationException {
		log("[onDeleteDirectory] " + path);
		File f = resolvePath(path);

		if (!MetaFileStore.removeMetaFile(f.getPath()))

			log.debug("unable to delete folder " + f.getPath());
		throw new DokanOperationException(WinError.ERROR_DIR_NOT_EMPTY);
	}

	public void onMoveFile(String from, String to,
			boolean replaceExisiting, DokanFileInfo arg3)
			throws DokanOperationException {
		log("==> [onMoveFile] " + from + " -> " + to
				+ ", replaceExisiting = " + replaceExisiting);
		File f = null;
		try {
			f = resolvePath(from);

			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			mf.renameTo(this.mountedVolume + to);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DokanOperationException(ERROR_FILE_EXISTS);
		} finally {
			f = null;
		}
		log("<== [onMoveFile]");
	}

	public void onLockFile(String fileName, long arg1, long arg2,
			DokanFileInfo arg3) throws DokanOperationException {
		log("[onLockFile] " + fileName);
	}

	public void onUnlockFile(String fileName, long arg1, long arg2,
			DokanFileInfo arg3) throws DokanOperationException {
		log("[onUnlockFile] " + fileName);
	}

	public DokanDiskFreeSpace onGetDiskFreeSpace(DokanFileInfo arg0)
			throws DokanOperationException {
		return null;
	}

	public DokanVolumeInformation onGetVolumeInformation(String arg0,
			DokanFileInfo arg1) throws DokanOperationException {
		return null;
	}

	public void onUnmount(DokanFileInfo arg0) throws DokanOperationException {
		log("[onUnmount]");
		Dokan.unmount(driveLetter);
	}

	private DedupFileChannel getFileChannel(String path)
			throws DokanOperationException {
		File f = this.resolvePath(path);
		try {
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			return mf.getDedupFile().getChannel();
		} catch (IOException e) {
			log.error("unable to open file" + f.getPath(), e);
			throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
		}
	}

	private File resolvePath(String path) throws DokanOperationException {
		File _f = new File(mountedVolume + path);
		if (!_f.exists()) {
			_f = null;
			log.debug("No such node");
			throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		}
		return _f;
	}
}
