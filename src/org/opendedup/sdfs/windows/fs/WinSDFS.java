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
import static net.decasdev.dokan.WinError.ERROR_ALREADY_EXISTS;
import static net.decasdev.dokan.WinError.ERROR_DISK_FULL;
import static net.decasdev.dokan.WinError.ERROR_FILE_EXISTS;
import static net.decasdev.dokan.WinError.ERROR_FILE_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_PATH_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_READ_FAULT;
import static net.decasdev.dokan.WinError.ERROR_WRITE_FAULT;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

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

import org.apache.log4j.Logger;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.MetaDataDedupFile;

public class WinSDFS implements DokanOperations {
	/** fileName -> MemFileInfo */
	// TODO FIX THIS
	public static final int FILE_CASE_PRESERVED_NAMES = 0x00000002;
	public static final int FILE_FILE_COMPRESSION = 0x00000010;
	public static final int FILE_SUPPORTS_SPARSE_FILES = 0x00000040;
	public static final int FILE_UNICODE_ON_DISK = 0x00000004;

	public static final int SUPPORTED_FLAGS = FILE_CASE_PRESERVED_NAMES
			| FILE_UNICODE_ON_DISK | FILE_SUPPORTS_SPARSE_FILES;
	final static int volumeSerialNumber = 64426442;
	/** Next handle */
	long nextHandleNo = 1;
	final long rootCreateTime = FileTimeUtils.toFileTime(new Date());
	long rootLastWrite = rootCreateTime;
	private String mountedVolume = null;
	private String driveLetter = "S:\\";
	private Logger log = SDFSLogger.getLog();
	TLongObjectHashMap<DedupFileChannel> dedupChannels = new TLongObjectHashMap<DedupFileChannel>(
			Main.maxOpenFiles + 1);

	/*
	 * private transient ConcurrentLinkedHashMap<String, DedupFileChannel>
	 * dedupChannels = new Builder<String, DedupFileChannel>()
	 * .concurrencyLevel(Main.writeThreads) .initialCapacity(Main.maxOpenFiles)
	 * .maximumWeightedCapacity(Main.maxOpenFiles + 1) .listener(new
	 * EvictionListener<String, DedupFileChannel>() { // This method is called
	 * just after a new entry has been // added public void onEviction(String
	 * key, DedupFileChannel ch) {
	 * 
	 * try { ch.close(); } catch (IOException e) { }
	 * 
	 * } }
	 * 
	 * ).build();
	 */
	static void logs(String msg) {
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

	void mount(String driveLetter, String mountedVolume) {
		Dokan.removeMountPoint(driveLetter);
		this.mountedVolume = mountedVolume;
		DokanOptions dokanOptions = new DokanOptions();
		dokanOptions.mountPoint = driveLetter;
		dokanOptions.threadCount = Main.writeThreads;
		this.driveLetter = driveLetter;
		log.info("######## mounting " + mountedVolume + " to "
				+ this.driveLetter + " #############");
		int result = Dokan.mount(dokanOptions, this);

		// log("[MemoryFS] result = " + result);
		if (result < 0) {
			System.out.println("Unable to mount volume because result = "
					+ result);
			log.error("Unable to mount volume because result = " + result);
			if (result == -1)
				System.out.println("General Error");
			if (result == -2)
				System.out.println("Bad Drive letter");
			if (result == -3)
				System.out.println("Can't install driver");
			if (result == -4)
				System.out.println("Driver something wrong");
			if (result == -5)
				System.out
						.println("Can't assign a drive letter or mount point");
			if (result == -6)
				System.out.println("Mount point is invalid");
			System.exit(-1);

		} else {
			log.info("######## mounted " + mountedVolume + " to "
					+ this.driveLetter + " with result " + result
					+ " #############");
		}
	}

	synchronized long getNextHandle() {
		if (nextHandleNo == Long.MAX_VALUE)
			nextHandleNo = 0;
		return nextHandleNo++;
	}

	@Override
	public long onCreateFile(String fileName, int desiredAccess, int shareMode,
			int creationDisposition, int flagsAndAttributes, DokanFileInfo arg5)
			throws DokanOperationException {

		// log("[onCreateFile] " + fileName + ", creationDisposition = "
		// + creationDisposition);
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
		} else if (new File(mountedVolume + fileName).exists()) {
			switch (creationDisposition) {
			case CREATE_NEW:
				throw new DokanOperationException(ERROR_ALREADY_EXISTS);
			case OPEN_ALWAYS:
			case OPEN_EXISTING:
				return getNextHandle();
			case CREATE_ALWAYS:
			case TRUNCATE_EXISTING:
				try {
					long fileHandle = getNextHandle();
					this.getFileChannel(fileName, fileHandle).truncateFile(0);
					this.closeFileChannel(fileHandle);
					return fileHandle;
				} catch (IOException e) {
					log.error(
							"unable to clear file "
									+ this.resolvePath(fileName).getPath(), e);
					throw new DokanOperationException(
							WinError.ERROR_WRITE_FAULT);
				}
			}
		} else {
			String path = mountedVolume + fileName;
			switch (creationDisposition) {

			case CREATE_NEW:
				if (Main.volume.isFull())
					throw new DokanOperationException(ERROR_DISK_FULL);
				try {

					log.debug("creating " + fileName);
					MetaDataDedupFile mf = MetaFileStore.getMF(path);
					mf.sync(true);
				} catch (Exception e) {
					log.error("unable to create file " + path, e);
					throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
				}
				return getNextHandle();
			case CREATE_ALWAYS:
			case OPEN_ALWAYS:
				if (Main.volume.isFull())
					throw new DokanOperationException(ERROR_DISK_FULL);
				try {
					log.debug("creating " + fileName);
					MetaDataDedupFile mf = MetaFileStore.getMF(path);
					mf.sync(true);
				} catch (Exception e) {
					log.error("unable to create file " + path, e);
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

	@Override
	public long onOpenDirectory(String pathName, DokanFileInfo arg1)
			throws DokanOperationException {
		// log("[onOpenDirectory] " + pathName);
		if (pathName.equals("\\"))
			return getNextHandle();
		pathName = Utils.trimTailBackSlash(pathName);
		File _f = new File(mountedVolume + pathName);
		if (_f.exists())
			return getNextHandle();
		else
			throw new DokanOperationException(ERROR_PATH_NOT_FOUND);
	}

	@Override
	public void onCreateDirectory(String pathName, DokanFileInfo file)
			throws DokanOperationException {
		if (Main.volume.isFull())
			throw new DokanOperationException(ERROR_DISK_FULL);
		// log("[onCreateDirectory] " + pathName);
		pathName = Utils.trimTailBackSlash(pathName);
		File f = new File(this.mountedVolume + pathName);
		if (f.exists()) {
			f = null;
			throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
		}
		f.mkdir();
	}

	@Override
	public void onCleanup(String arg0, DokanFileInfo arg2)
			throws DokanOperationException {
	}

	@Override
	public void onCloseFile(String path, DokanFileInfo arg1)
			throws DokanOperationException {
		// log("[onClose] " + path);
		path = Utils.trimTailBackSlash(path);
		File f = new File(this.mountedVolume + path);
		if (!f.isDirectory()) {
			this.closeFileChannel(arg1.handle);
		}
	}

	@Override
	public int onReadFile(String fileName, ByteBuffer buf, long offset,
			DokanFileInfo arg3) throws DokanOperationException {
		DedupFileChannel ch = this.getFileChannel(fileName, arg3.handle);
		try {
			int read = ch.read(buf, 0, buf.capacity(), offset);
			if (read == -1)
				read = 0;
			return read;
		} catch (Exception e) {
			log.error("unable to read file " + fileName, e);
			throw new DokanOperationException(ERROR_READ_FAULT);
		}
	}

	@Override
	public int onWriteFile(String fileName, ByteBuffer buf, long offset,
			DokanFileInfo arg3) throws DokanOperationException {
		if (Main.volume.isFull())
			throw new DokanOperationException(ERROR_DISK_FULL);
		// log("[onWriteFile] " + fileName);
		DedupFileChannel ch = this.getFileChannel(fileName, arg3.handle);
		try {
			ch.writeFile(buf, buf.capacity(), 0, offset, true);
			// log("wrote " + new String(b));
		} catch (IOException e) {
			log.error("unable to write to file" + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
		return buf.capacity();
	}

	@Override
	public void onSetEndOfFile(String fileName, long length, DokanFileInfo arg2)
			throws DokanOperationException {
		// log("[onSetEndOfFile] " + fileName);
		DedupFileChannel ch = this.getFileChannel(fileName, arg2.handle);
		try {
			ch.truncateFile(length);
		} catch (IOException e) {
			log.error("Unable to set length  of " + fileName + " to " + length);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public void onFlushFileBuffers(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {
		DedupFileChannel ch = this.getFileChannel(fileName, arg1.handle);
		try {
			ch.force(true);
		} catch (Exception e) {
			log.error("unable to sync file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}

	}

	@Override
	public ByHandleFileInformation onGetFileInformation(String fileName,
			DokanFileInfo arg1) throws DokanOperationException {
		log.debug("[onGetFileInformation] " + fileName);
		if (fileName.equals("\\")) {
			return new ByHandleFileInformation(FILE_ATTRIBUTE_NORMAL
					| FILE_ATTRIBUTE_DIRECTORY, rootCreateTime, rootCreateTime,
					rootLastWrite, volumeSerialNumber,
					Main.volume.getCapacity(), 1, 1);
		}
		try {
		MetaDataDedupFile mf = MetaFileStore.getMF(this.resolvePath(fileName)
				.getPath());
		MetaDataFileInfo fi = new MetaDataFileInfo(fileName, mf);
		return fi.toByHandleFileInformation();
		}catch (Exception e) {
			log.error("unable to sync file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public Win32FindData[] onFindFiles(String pathName, DokanFileInfo arg1)
			throws DokanOperationException {
		// log("[onFindFiles] " + pathName);
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
			// log("[onFindFiles] " + files);
			return files.toArray(new Win32FindData[0]);
		} catch (Exception e) {
			log.error("unable to list files for " + pathName, e);
			throw new DokanOperationException(WinError.ERROR_DIRECTORY);
		} finally {
			f = null;
		}
	}

	@Override
	public Win32FindData[] onFindFilesWithPattern(String arg0, String arg1,
			DokanFileInfo arg2) throws DokanOperationException {
		return null;
	}

	@Override
	public void onSetFileAttributes(String fileName, int fileAttributes,
			DokanFileInfo arg2) throws DokanOperationException {
		// log("[onSetFileAttributes] " + fileName);
		/*
		 * MemFileInfo fi = fileInfoMap.get(fileName); if (fi == null) throw new
		 * DokanOperationException(ERROR_FILE_NOT_FOUND); fi.fileAttribute =
		 * fileAttributes;
		 */
	}

	@Override
	public void onSetFileTime(String fileName, long creationTime, long atime,
			long mtime, DokanFileInfo arg4) throws DokanOperationException {
		// log("[onSetFileTime] " + fileName);
		File f = this.resolvePath(fileName);
		try {
		MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
		mf.setLastAccessed(atime * 1000L, true);
		mf.setLastModified(mtime * 1000L, true);
		}catch (Exception e) {
			log.error("unable to set file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public void onDeleteFile(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {
		// log("[onDeleteFile] " + fileName);

		DedupFileChannel ch = this.getFileChannel(fileName, arg1.handle);
		if (ch != null) {
			this.channelLock.lock();
			try {
				this.closeFileChannel(arg1.handle);
			} catch (Exception e) {
				log.error("unable to close " + fileName, e);
			} finally {
				this.channelLock.unlock();
			}
		}
		File f = this.resolvePath(fileName);
		if (!MetaFileStore.removeMetaFile(f.getPath(), true)) {
			log.warn("unable to delete file " + f.getPath());
			throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
		}
		//
	}

	@Override
	public void onDeleteDirectory(String path, DokanFileInfo arg1)
			throws DokanOperationException {
		// log("[onDeleteDirectory] " + path);
		File f = resolvePath(path);
		if (!MetaFileStore.removeMetaFile(f.getPath(), true)) {
			log.debug("unable to delete folder " + f.getPath());
			throw new DokanOperationException(WinError.ERROR_DIR_NOT_EMPTY);
		}
	}

	@Override
	public void onMoveFile(String from, String to, boolean replaceExisiting,
			DokanFileInfo arg3) throws DokanOperationException {
		// log("==> [onMoveFile] " + from + " -> " + to +
		// ", replaceExisiting = "
		// + replaceExisiting);
		File f = null;
		try {
			f = resolvePath(from);
			MetaFileStore.rename(f.getPath(),this.mountedVolume + to);
			DedupFileChannel ch = this.dedupChannels.get(arg3.handle);
			if (ch != null) {
				this.channelLock.lock();
				try {
					ch.getDedupFile().unRegisterChannel(ch, -1);
					this.dedupChannels.remove(arg3.handle);
				} catch (Exception e) {
					log.error("unable to close " + from, e);
				} finally {
					this.channelLock.unlock();
				}
			}
		} catch (Exception e) {
			log.error("unable to move file " + from + " to " + to, e);
			throw new DokanOperationException(ERROR_FILE_EXISTS);
		} finally {
			f = null;
		}
		// log("<== [onMoveFile]");
	}

	@Override
	public void onLockFile(String fileName, long arg1, long arg2,
			DokanFileInfo arg3) throws DokanOperationException {
		// log("[onLockFile] " + fileName);
	}

	@Override
	public void onUnlockFile(String fileName, long arg1, long arg2,
			DokanFileInfo arg3) throws DokanOperationException {
		// log("[onUnlockFile] " + fileName);
	}

	@Override
	public DokanDiskFreeSpace onGetDiskFreeSpace(DokanFileInfo arg0)
			throws DokanOperationException {
		DokanDiskFreeSpace free = new DokanDiskFreeSpace();
		free.freeBytesAvailable = Main.volume.getCapacity()
				- Main.volume.getCurrentSize();
		free.totalNumberOfBytes = Main.volume.getCapacity();
		free.totalNumberOfFreeBytes = Main.volume.getCapacity()
				- Main.volume.getCurrentSize();
		return free;
	}

	@Override
	public DokanVolumeInformation onGetVolumeInformation(String arg0,
			DokanFileInfo arg1) throws DokanOperationException {
		DokanVolumeInformation info = new DokanVolumeInformation();
		info.fileSystemFlags = SUPPORTED_FLAGS;
		info.maximumComponentLength = 256;
		info.volumeName = "Dedup Filesystem";
		info.fileSystemName = "SDFS";
		info.volumeSerialNumber = volumeSerialNumber;
		return info;
	}

	@Override
	public void onUnmount(DokanFileInfo arg0) throws DokanOperationException {
		// log("[onUnmount]");
		Dokan.removeMountPoint(driveLetter);
		TLongObjectIterator<DedupFileChannel> iter = this.dedupChannels
				.iterator();
		while (iter.hasNext()) {
			try {
				iter.value().getDedupFile().unRegisterChannel(iter.value(), -1);
			} catch (Exception e) {

			}
		}
	}

	private ReentrantLock channelLock = new ReentrantLock();

	private DedupFileChannel getFileChannel(String path, long handleNo)
			throws DokanOperationException {
		DedupFileChannel ch = this.dedupChannels.get(handleNo);
		if (ch == null) {
			File f = this.resolvePath(path);
			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
				ch = mf.getDedupFile().getChannel(-1);
				channelLock.lock();
				try {
					if (this.dedupChannels.containsKey(handleNo)) {
						ch.getDedupFile().unRegisterChannel(ch, -1);
						ch = this.dedupChannels.get(handleNo);
					} else {
						this.dedupChannels.put(handleNo, ch);
					}
				} catch (Exception e) {

				} finally {
					log.debug("number of channels is "
							+ this.dedupChannels.size());
					channelLock.unlock();
				}
			} catch (Exception e) {
				log.error("unable to open file" + f.getPath(), e);
				throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
			}
		}
		return ch;
	}

	private void closeFileChannel(long handleNo) {
		DedupFileChannel ch = this.dedupChannels.remove(handleNo);
		if (ch != null) {
			try {
				ch.getDedupFile().unRegisterChannel(ch, -1);
			} catch (Exception e) {
				log.error("unable to close channel" + handleNo, e);
			} finally {
				log.debug("number of channels is " + this.dedupChannels.size());
			}

		}
	}

	/*
	 * private DedupFile getDedupFile(String path) throws
	 * DokanOperationException { File f = this.resolvePath(path); try {
	 * MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath()); return
	 * mf.getDedupFile(); } catch (IOException e) {
	 * log.error("unable to open file" + f.getPath(), e); throw new
	 * DokanOperationException(WinError.ERROR_GEN_FAILURE); } }
	 */

	private File resolvePath(String path) throws DokanOperationException {
		File _f = new File(mountedVolume + path);
		if (!_f.exists()) {
			_f = null;
			log.debug("No such node " + path);
			throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		}
		return _f;
	}
}
