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

import static net.decasdev.dokan.WinError.ERROR_ALREADY_EXISTS;
import static net.decasdev.dokan.WinError.ERROR_GEN_FAILURE;
import static net.decasdev.dokan.WinError.ERROR_DISK_FULL;
import static net.decasdev.dokan.WinError.ERROR_FILE_EXISTS;
import static net.decasdev.dokan.WinError.ERROR_FILE_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_PATH_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_READ_FAULT;
import static net.decasdev.dokan.WinError.ERROR_WRITE_FAULT;
import static net.decasdev.dokan.FileAttribute.FileAttributeFlags.FILE_ATTRIBUTE_DIRECTORY;
import static net.decasdev.dokan.FileAttribute.FileAttributeFlags.FILE_ATTRIBUTE_NORMAL;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.CreationDisposition;
import net.decasdev.dokan.Dokan;
import net.decasdev.dokan.DokanDiskFreeSpace;
import net.decasdev.dokan.DokanFileInfo;
import net.decasdev.dokan.DokanOperationException;
import net.decasdev.dokan.DokanOperations;
import net.decasdev.dokan.DokanOptionsMode;
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
	/** Next handle */
	long nextHandleNo = 1;
	final long rootCreateTime = FileTimeUtils.toFileTime(new Date());
	long rootLastWrite = rootCreateTime;
	private String mountedVolume = null;
	private String driveLetter = "S:\\";
	private Logger log = SDFSLogger.getLog();
	ConcurrentHashMap<Long, DedupFileChannel> dedupChannels = new ConcurrentHashMap<Long, DedupFileChannel>();

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

		this.mountedVolume = mountedVolume;
		DokanOptions dokanOptions = new DokanOptions();
		dokanOptions.optionsMode = DokanOptionsMode.Mode.KEEP_ALIVE.getValue() + DokanOptionsMode.Mode.REMOVABLE_DRIVE.getValue();
		dokanOptions.mountPoint = driveLetter;
		dokanOptions.threadCount = 0;
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
			Collection<DedupFileChannel> iter = this.dedupChannels.values();
			for (DedupFileChannel ch : iter) {
				try {
					ch.getDedupFile().unRegisterChannel(ch, -1);
				} catch (Exception e) {

				}
			}

			log.info("######## unmounted " + mountedVolume + " from "
					+ this.driveLetter + " #############");
			System.exit(1);
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
		try {
			CreationDisposition disposition = CreationDisposition
					.build(creationDisposition);
			log.debug("[onCreateFile] " + fileName + ", creationDisposition = "
					+ disposition + " shareMode=" + shareMode
					+ " desiredAccess=" + desiredAccess
					+ " flagsAndAttributes=" + flagsAndAttributes);

			if (fileName.equals("\\")) {
				switch (disposition) {
				case CREATE_NEW:
					throw new DokanOperationException(ERROR_ALREADY_EXISTS);
				case CREATE_ALWAYS:
					throw new DokanOperationException(ERROR_ALREADY_EXISTS);
				case OPEN_ALWAYS:
					return getNextHandle();
				case OPEN_EXISTING:
					return getNextHandle();
				case TRUNCATE_EXISTING:
					return getNextHandle();
				case UNDEFINED:
					assert (false);
				}
			} else if (new File(mountedVolume + fileName).exists()) {
				switch (disposition) {
				case CREATE_NEW:
					throw new DokanOperationException(ERROR_ALREADY_EXISTS);
				case OPEN_ALWAYS:
					return getNextHandle();
				case OPEN_EXISTING:
					return getNextHandle();
				case CREATE_ALWAYS:
					try {
						long fileHandle = getNextHandle();
						this.getFileChannel(fileName, fileHandle).truncateFile(
								0);
						this.closeFileChannel(fileHandle);
						return fileHandle;
					} catch (IOException e) {
						log.error(
								"unable to clear file "
										+ this.resolvePath(fileName).getPath(),
								e);
						throw new DokanOperationException(
								WinError.ERROR_WRITE_FAULT);
					}
				case TRUNCATE_EXISTING:
					try {
						long fileHandle = getNextHandle();
						this.getFileChannel(fileName, fileHandle).truncateFile(
								0);
						this.closeFileChannel(fileHandle);
						return fileHandle;
					} catch (IOException e) {
						log.error(
								"unable to clear file "
										+ this.resolvePath(fileName).getPath(),
								e);
						throw new DokanOperationException(
								WinError.ERROR_WRITE_FAULT);
					}
				case UNDEFINED:
					assert (false);
				}
			} else {
				String path = mountedVolume + fileName;
				switch (disposition) {

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
					throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
				case TRUNCATE_EXISTING:
					throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
				case UNDEFINED:
					assert (false);
				}
			}
			throw new DokanOperationException(WinError.ERROR_INVALID_FUNCTION);
		} catch (DokanOperationException e) {
			log.debug("dokan error", e);
			throw e;
		} catch (Exception e) {
			log.error("unable to create file", e);
			throw new DokanOperationException(WinError.ERROR_INVALID_FUNCTION);
		}
	}

	@Override
	public long onOpenDirectory(String pathName, DokanFileInfo arg1)
			throws DokanOperationException {
		try {
			log.debug("[onOpenDirectory] " + pathName);
			if (pathName.equals("\\"))
				return getNextHandle();
			pathName = Utils.trimTailBackSlash(pathName);
			File _f = new File(mountedVolume + pathName);
			if (_f.exists())
				return getNextHandle();
			else
				throw new DokanOperationException(ERROR_PATH_NOT_FOUND);
		} catch (DokanOperationException e) {
			log.debug("dokan error", e);
			throw e;
		} catch (Exception e) {
			log.error("unable to create directory", e);
			throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
		}
	}

	@Override
	public void onCreateDirectory(String pathName, DokanFileInfo file)
			throws DokanOperationException {
		try {
			if (Main.volume.isFull())
				throw new DokanOperationException(ERROR_DISK_FULL);
			log.debug("[onCreateDirectory] " + pathName);
			pathName = Utils.trimTailBackSlash(pathName);
			File f = new File(this.mountedVolume + pathName);
			if (f.exists()) {
				f = null;
				throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
			}
			f.mkdir();
		} catch (DokanOperationException e) {
			log.debug("dokan error", e);
			throw e;
		} catch (Exception e) {
			log.error("unable to create directory", e);
			throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
		}
	}

	@Override
	public void onCleanup(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {
		if (!fileName.equals("\\")) {
			try {
				log.debug("[onCleanup] " + fileName);
				DedupFileChannel ch = this
						.getFileChannel(fileName, arg1.handle);
				ch.force(true);
			} catch (Exception e) {
				log.error("unable to cleanup file " + fileName, e);
				throw new DokanOperationException(ERROR_WRITE_FAULT);
			}
		}
	}

	@Override
	public void onCloseFile(String path, DokanFileInfo arg1)
			throws DokanOperationException {
		if (!path.equals("\\")) {
		try {
			log.debug("[onClose] " + path);
			this.closeFileChannel(arg1.handle);
		} catch (Exception e) {
			log.error("unable to close file " + path, e);
		}
		}
	}

	@Override
	public int onReadFile(String fileName, ByteBuffer buf, long offset,
			DokanFileInfo arg3) throws DokanOperationException {

		try {
			log.debug("[onReadFile] " + fileName);
			DedupFileChannel ch = this.getFileChannel(fileName, arg3.handle);
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
		try {
			if (Main.volume.isFull())
				throw new DokanOperationException(ERROR_DISK_FULL);
			log.debug("[onWriteFile] " + fileName + " sz=" + buf.capacity());
			DedupFileChannel ch = this.getFileChannel(fileName, arg3.handle);
			/*
			 * WriteThread th = new WriteThread(); th.buf = buf; th.offset =
			 * offset; th.ch = ch; executor.execute(th);
			 */
			ch.writeFile(buf, buf.capacity(), 0, offset, true);
			return buf.position();
			// log("wrote " + new String(b));
		} catch (DokanOperationException e) {
			log.error("Unable to write " + fileName + " at " + offset, e);
			throw e;
		} catch (Exception e) {
			log.error("Unable to write " + fileName + " at " + offset, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}

	}

	@Override
	public void onSetEndOfFile(String fileName, long length, DokanFileInfo arg2)
			throws DokanOperationException {

		try {
			log.debug("[onSetEndOfFile] " + fileName);
			DedupFileChannel ch = this.getFileChannel(fileName, arg2.handle);
			ch.truncateFile(length);
		} catch (Exception e) {
			log.error("Unable to set length  of " + fileName + " to " + length);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public void onFlushFileBuffers(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {

		try {
			log.debug("[onFlushFileBuffers] " + fileName);
			DedupFileChannel ch = this.getFileChannel(fileName, arg1.handle);
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
		try {
			if (fileName.equals("\\")) {
				return new ByHandleFileInformation(
						FILE_ATTRIBUTE_NORMAL.getValue()
								| FILE_ATTRIBUTE_DIRECTORY.getValue(),
						rootCreateTime, rootCreateTime, rootLastWrite,
						Main.volume.getSerialNumber(),
						Main.volume.getCapacity(), 1, 1);
			}

			MetaDataDedupFile mf = MetaFileStore.getMF(this.resolvePath(
					fileName).getPath());
			MetaDataFileInfo fi = new MetaDataFileInfo(fileName, mf);
			return fi.toByHandleFileInformation();
		} catch (Exception e) {
			log.error("unable to sync file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public Win32FindData[] onFindFiles(String pathName, DokanFileInfo arg1)
			throws DokanOperationException {
		log.debug("[onFindFiles] " + pathName);
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
	public Win32FindData[] onFindFilesWithPattern(String pathName, String arg1,
			DokanFileInfo arg2) throws DokanOperationException {
		log.debug("[onFindFilesWithPattern] " + pathName);
		return null;
	}

	@Override
	public void onSetFileAttributes(String fileName, int fileAttributes,
			DokanFileInfo arg2) throws DokanOperationException {
		log.debug("[onSetFileAttributes] " + fileName);
		/*
		 * MemFileInfo fi = fileInfoMap.get(fileName); if (fi == null) throw new
		 * DokanOperationException(ERROR_FILE_NOT_FOUND); fi.fileAttribute =
		 * fileAttributes;
		 */
	}

	@Override
	public void onSetFileTime(String fileName, long creationTime, long atime,
			long mtime, DokanFileInfo arg4) throws DokanOperationException {
		log.debug("[onSetFileTime] " + fileName);

		try {
			File f = this.resolvePath(fileName);
			MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
			mf.setLastAccessed(atime * 1000L, true);
			mf.setLastModified(mtime * 1000L, true);
		} catch (Exception e) {
			log.error("unable to set file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public void onDeleteFile(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {
		log.debug("[onDeleteFile] " + fileName);
		try {
			DedupFileChannel ch = this.getFileChannel(fileName, arg1.handle);
			if (ch != null) {
				try {
					this.closeFileChannel(arg1.handle);
				} catch (Exception e) {
					log.error("unable to close " + fileName, e);
				}
			}
			File f = this.resolvePath(fileName);

			if (!MetaFileStore.removeMetaFile(f.getPath(), true)) {
				log.warn("unable to delete file " + f.getPath());
				throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
			}
		} catch (Exception e) {
			log.error("unable to delete folder " + fileName);
			throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
		}
		//
	}

	@Override
	public void onDeleteDirectory(String path, DokanFileInfo arg1)
			throws DokanOperationException {
		log.debug("[onDeleteDirectory] " + path);
		try {
			File f = resolvePath(path);

			if (!MetaFileStore.removeMetaFile(f.getPath(), true)) {
				log.error("unable to delete folder " + f.getPath());
				throw new DokanOperationException(WinError.ERROR_DIR_NOT_EMPTY);
			}
		} catch (Exception e) {
			log.error("unable to delete folder " + path);
			throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
		}
	}

	@Override
	public void onMoveFile(String from, String to, boolean replaceExisiting,
			DokanFileInfo arg3) throws DokanOperationException {
		log.debug("==> [onMoveFile] " + from + " -> " + to
				+ ", replaceExisiting = " + replaceExisiting);
		File f = null;
		try {
			f = resolvePath(from);
			MetaFileStore.rename(f.getPath(), this.mountedVolume + to);
			DedupFileChannel ch = this.dedupChannels.get(arg3.handle);
			if (ch != null) {

				try {
					ch.getDedupFile().unRegisterChannel(ch, -1);
					this.dedupChannels.remove(arg3.handle);
				} catch (Exception e) {
					log.error("unable to close " + from, e);
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
		log.debug("[onLockFile] " + fileName);
	}

	@Override
	public void onUnlockFile(String fileName, long arg1, long arg2,
			DokanFileInfo arg3) throws DokanOperationException {
		log.debug("[onUnlockFile] " + fileName);
	}

	@Override
	public DokanDiskFreeSpace onGetDiskFreeSpace(DokanFileInfo arg0)
			throws DokanOperationException {
		log.debug("[onGetDiskFreeSpace]");
		try {
			DokanDiskFreeSpace free = new DokanDiskFreeSpace();
			free.freeBytesAvailable = Main.volume.getCapacity()
					- Main.volume.getCurrentSize();
			free.totalNumberOfBytes = Main.volume.getCapacity();
			free.totalNumberOfFreeBytes = Main.volume.getCapacity()
					- Main.volume.getCurrentSize();
			return free;
		} catch (Exception e) {
			log.error("error getting free disk space", e);
			throw new DokanOperationException(ERROR_GEN_FAILURE);
		}
	}

	@Override
	public DokanVolumeInformation onGetVolumeInformation(String arg0,
			DokanFileInfo arg1) throws DokanOperationException {
		log.debug("[onGetVolumeInformation]");
		try {
			DokanVolumeInformation info = new DokanVolumeInformation();
			info.fileSystemFlags = SUPPORTED_FLAGS;
			info.maximumComponentLength = 256;
			info.volumeName = "Dedup Filesystem";
			info.fileSystemName = "SDFS";
			info.volumeSerialNumber = Main.volume.getSerialNumber();
			return info;
		} catch (Exception e) {
			log.error("error getting volume info", e);
			throw new DokanOperationException(ERROR_GEN_FAILURE);
		}
	}

	@Override
	public void onUnmount(DokanFileInfo arg0) throws DokanOperationException {
		log.debug("[onUnmount]");
		Dokan.removeMountPoint(driveLetter);
		Collection<DedupFileChannel> iter = this.dedupChannels.values();
		for (DedupFileChannel ch : iter) {
			try {
				ch.getDedupFile().unRegisterChannel(ch, -1);
			} catch (Exception e) {

			}
		}
		System.exit(1);
	}

	private DedupFileChannel getFileChannel(String path, long handleNo)
			throws DokanOperationException {
		DedupFileChannel ch = this.dedupChannels.get(handleNo);
		if (ch == null) {
			File f = this.resolvePath(path);
			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
				ch = mf.getDedupFile().getChannel(-1);
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
			log.error("No such node " + path);
			throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		}
		return _f;
	}
}
