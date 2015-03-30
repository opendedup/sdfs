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
//import static net.decasdev.dokan.WinError.ERROR_IO_PENDING;
import static net.decasdev.dokan.WinError.ERROR_NOT_ENOUGH_MEMORY;
import static net.decasdev.dokan.FileAttribute.FileAttributeFlags.FILE_ATTRIBUTE_DIRECTORY;
import static net.decasdev.dokan.FileAttribute.FileAttributeFlags.FILE_ATTRIBUTE_NORMAL;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
	public static final int FILE_SUPPORTS_SPARSE_FILES = 0x00000040;
	public static final int FILE_UNICODE_ON_DISK = 0x00000004;
	public static final int SUPPORTED_FLAGS = FILE_CASE_PRESERVED_NAMES
			| FILE_UNICODE_ON_DISK | FILE_SUPPORTS_SPARSE_FILES;
	/** Next handle */
	long nextHandleNo = 1;
	final long rootCreateTime = FileTimeUtils.toFileTime(new Date());
	long rootLastWrite = rootCreateTime;
	private static String mountedVolume = null;
	private static String driveLetter = "S:\\";
	private static Logger log = SDFSLogger.getLog();
	ConcurrentHashMap<Long, DedupFileChannel> dedupChannels = new ConcurrentHashMap<Long, DedupFileChannel>();
	private static BlockingQueue<Runnable> worksQueue = new LinkedBlockingQueue<Runnable>(
			1024);
	private static ThreadPoolExecutor executor = new ThreadPoolExecutor(
			Main.writeThreads, Main.writeThreads, 10, TimeUnit.SECONDS,
			worksQueue);
	private static AtomicInteger activeTh = new AtomicInteger(0);
	private static AtomicInteger readTh = new AtomicInteger(0);
	private static AtomicInteger wrTh = new AtomicInteger(0);
	private static AtomicInteger syncTh = new AtomicInteger(0);
	private static AtomicInteger clTh = new AtomicInteger(0);
	private static final int CHANNEL_TIMEOUT = 10 * 1000;

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

	public WinSDFS() {
		showVersions();
	}

	void showVersions() {
		//int version = Dokan.getVersion();
		//System.out.println("version = " + version);
		//int driverVersion = Dokan.getDriverVersion();
		//System.out.println("driverVersion = " + driverVersion);
	}

	void mount(String _driveLetter, String _mountedVolume) {

		mountedVolume = _mountedVolume;
		driveLetter = _driveLetter;
		DokanOptions dokanOptions = new DokanOptions();
		dokanOptions.optionsMode = DokanOptionsMode.Mode.KEEP_ALIVE.getValue();
		dokanOptions.mountPoint = driveLetter;
		dokanOptions.threadCount = Main.writeThreads;
		
		log.info("######## mounting " + mountedVolume + " to " + driveLetter
				+ " #############");

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
					+ driveLetter + " #############");
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
			if (SDFSLogger.isDebug())
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
						truncateFile(this.getFileChannel(fileName, fileHandle),0,arg5);
						this.closeFileChannel(fileHandle, arg5);
						return fileHandle;
					} catch (IOException e) {
						log.error(
								"unable to clear file "
										+ resolvePath(fileName).getPath(), e);
						throw new DokanOperationException(
								WinError.ERROR_WRITE_FAULT);
					}
				case TRUNCATE_EXISTING:
					try {
						long fileHandle = getNextHandle();
						truncateFile(this.getFileChannel(fileName, fileHandle),0,arg5);
						this.closeFileChannel(fileHandle, arg5);
						return fileHandle;
					} catch (IOException e) {
						log.error(
								"unable to clear file "
										+ resolvePath(fileName).getPath(), e);
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

						if (SDFSLogger.isDebug())
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

						if (SDFSLogger.isDebug())
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
						if (SDFSLogger.isDebug())
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
			if (SDFSLogger.isDebug())
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
			if (SDFSLogger.isDebug())
				log.debug("[onCreateDirectory] " + pathName);
			pathName = Utils.trimTailBackSlash(pathName);
			MkDirThread sn = new MkDirThread();
			sn.pathName = pathName;
			try {
				executor.execute(sn);
				while (!sn.done) {
					synchronized (sn) {
						sn.wait(CHANNEL_TIMEOUT);
					}
					if (!sn.done) {
						Dokan.resetTimeout(15000, file);
						// log.warn("sync did not finish in 5 seconds. slow io.");
					}
				}
				if (sn.errRtn != null)
					throw sn.errRtn;
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
			}
		} catch (DokanOperationException e) {
			//log.debug("dokan error", e);
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
				if (SDFSLogger.isDebug())
					log.debug("[onCleanup] " + fileName);
				DedupFileChannel ch = this
						.getFileChannel(fileName, arg1.handle);
				SyncThread sn = new SyncThread();
				sn.ch = ch;
				try {
					executor.execute(sn);
					while (!sn.done) {
						synchronized (sn) {
							sn.wait(CHANNEL_TIMEOUT);
						}
						if (!sn.done) {
							Dokan.resetTimeout(15000, arg1);
							// log.warn("sync did not finish in 5 seconds. slow io.");
						}
					}
					if (sn.errRtn != null)
						throw sn.errRtn;
				} catch (RejectedExecutionException e) {
					log.warn("Threads exhausted");
					throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
				}
				// ch.force(true);
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
				if (SDFSLogger.isDebug())
					log.debug("[onClose] " + path);
				this.closeFileChannel(arg1.handle, arg1);
			} catch (Exception e) {
				log.error("unable to close file " + path, e);
			}
		}
	}

	@Override
	public int onReadFile(String fileName, ByteBuffer buf, long offset,
			DokanFileInfo arg3) throws DokanOperationException {

		try {
			if (SDFSLogger.isDebug())
				log.debug("[onReadFile] " + fileName);
			DedupFileChannel ch = this.getFileChannel(fileName, arg3.handle);
			ReadThread wr = new ReadThread();
			wr.buf = buf;
			wr.ch = ch;
			wr.pos = offset;
			try {
				executor.execute(wr);
				while (!wr.done) {
					synchronized (wr) {
						wr.wait(CHANNEL_TIMEOUT);
					}
					if (!wr.done) {
						// log.warn("write did not finish in 5 seconds. slow io.");
						Dokan.resetTimeout(15000, arg3);
					}
				}
				if (wr.errRtn != null)
					throw wr.errRtn;
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
			}
			// int read = ch.read(buf, 0, buf.capacity(), offset);
			// if (read == -1)
			// read = 0;
			return buf.position();
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
			// log.info("[onWriteFile] " + fileName + " sz=" + buf.capacity());
			DedupFileChannel ch = this.getFileChannel(fileName, arg3.handle);
			/*
			 * WriteThread th = new WriteThread(); th.buf = buf; th.offset =
			 * offset; th.ch = ch; executor.execute(th);
			 */
			WriteThread wr = new WriteThread();
			wr.buf = buf;
			wr.ch = ch;
			wr.pos = offset;
			try {
				executor.execute(wr);
				while (!wr.done) {
					synchronized (wr) {
						wr.wait(CHANNEL_TIMEOUT);
					}
					if (!wr.done) {
						// log.warn("write did not finish in 5 seconds. slow io.");
						Dokan.resetTimeout(15000, arg3);
					}
				}
				if (wr.errRtn != null)
					throw wr.errRtn;
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
				// throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
			}
			// ch.writeFile(buf, buf.capacity(), 0, offset, true);
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
			if (SDFSLogger.isDebug())
				log.debug("[onSetEndOfFile] " + fileName);
			DedupFileChannel ch = this.getFileChannel(fileName, arg2.handle);
			truncateFile(ch,length,arg2);
		} catch (Exception e) {
			log.error("Unable to set length  of " + fileName + " to " + length);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public void onFlushFileBuffers(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {

		try {
			if (SDFSLogger.isDebug())
				log.debug("[onFlushFileBuffers] " + fileName);
			DedupFileChannel ch = this.getFileChannel(fileName, arg1.handle);
			SyncThread sn = new SyncThread();
			sn.ch = ch;
			try {
				executor.execute(sn);
				while (!sn.done) {
					synchronized (sn) {
						sn.wait(CHANNEL_TIMEOUT);
					}
					if (!sn.done) {
						// log.warn("sync did not finish in 5 seconds. slow io.");
						Dokan.resetTimeout(15000, arg1);
					}
				}
				if (sn.errRtn != null)
					throw sn.errRtn;
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
			}
		} catch (Exception e) {

			log.error("unable to sync file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}

	}

	@Override
	public ByHandleFileInformation onGetFileInformation(String fileName,
			DokanFileInfo arg1) throws DokanOperationException {
		if (SDFSLogger.isDebug())
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

			MetaDataDedupFile mf = MetaFileStore.getMF(resolvePath(fileName)
					.getPath());
			MetaDataFileInfo fi = new MetaDataFileInfo(fileName, mf);
			return fi.toByHandleFileInformation();
		} catch (Exception e) {
			log.error("unable to get file info " + fileName, e);
			throw new DokanOperationException(ERROR_GEN_FAILURE);
		}
	}

	@Override
	public Win32FindData[] onFindFiles(String pathName, DokanFileInfo arg1)
			throws DokanOperationException {
		try {
			if (SDFSLogger.isDebug())
				log.debug("[onFindFiles] " + pathName);
			ListFiles sn = new ListFiles();
			sn.pathName = pathName;
			try {
				executor.execute(sn);
				while (!sn.done) {
					synchronized (sn) {
						sn.wait(CHANNEL_TIMEOUT);
					}
					if (!sn.done) {
						// log.warn("sync did not finish in 5 seconds. slow io.");
						Dokan.resetTimeout(15000, arg1);
					}
				}
				if (sn.errRtn != null)
					throw sn.errRtn;
				else
					return sn.filedata;
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
			}
		} catch (Exception e) {

			log.error("unable to list file " + pathName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public Win32FindData[] onFindFilesWithPattern(String pathName, String arg1,
			DokanFileInfo arg2) throws DokanOperationException {
		if (SDFSLogger.isDebug())
			log.debug("[onFindFilesWithPattern] " + pathName);
		return null;
	}

	@Override
	public void onSetFileAttributes(String fileName, int fileAttributes,
			DokanFileInfo arg2) throws DokanOperationException {
		if (SDFSLogger.isDebug())
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
		if (SDFSLogger.isDebug())
			log.debug("[onSetFileTime] " + fileName);

		try {
			SetTimeThread sn = new SetTimeThread();
			sn.fileName = fileName;
			sn.atime = atime;
			sn.mtime = mtime;
			try {
				executor.execute(sn);
				while (!sn.done) {
					synchronized (sn) {
						sn.wait(CHANNEL_TIMEOUT);
					}
					if (!sn.done) {
						// log.warn("sync did not finish in 5 seconds. slow io.");
						Dokan.resetTimeout(15000, arg4);
					}
				}
				if (sn.errRtn != null)
					throw sn.errRtn;
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
			}

		} catch (DokanOperationException e) {
			throw e;
		} catch (Exception e) {
			log.error("unable to set file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}
	
	private void truncateFile(DedupFileChannel ch, long sz,DokanFileInfo nfo) throws Exception {
		TruncateThread wr = new TruncateThread();
		wr.ch = ch;
		wr.sz = sz;
		try {
			executor.execute(wr);
			while (!wr.done) {
				synchronized (wr) {
					wr.wait(CHANNEL_TIMEOUT);
				}
				if (!wr.done) {
					// log.warn("write did not finish in 5 seconds. slow io.");
					Dokan.resetTimeout(15000, nfo);
				}
			}
			if (wr.errRtn != null)
				throw wr.errRtn;
		} catch (RejectedExecutionException e) {
			log.warn("Threads exhausted");
			throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
			// throw new DokanOperationException(ERROR_NOT_ENOUGH_MEMORY);
		}
	}

	@Override
	public void onDeleteFile(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {
		if (SDFSLogger.isDebug())
			log.debug("[onDeleteFile] " + fileName);
		try {
			DedupFileChannel ch = this.getFileChannel(fileName, arg1.handle);
			if (ch != null) {
				try {
					this.closeFileChannel(arg1.handle, arg1);
				} catch (Exception e) {
					log.error("unable to close " + fileName, e);
				}
			}
			DeleteFileThread sn = new DeleteFileThread();
			sn.fileName = fileName;
			executor.execute(sn);
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					// log.warn("sync did not finish in 5 seconds. slow io.");
					Dokan.resetTimeout(15000, arg1);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;
		} catch (DokanOperationException e) {
			throw e;
		} catch (Exception e) {
			log.error("unable to delete file " + fileName, e);
			throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
		}
	}

	@Override
	public void onDeleteDirectory(String path, DokanFileInfo arg1)
			throws DokanOperationException {
		if (SDFSLogger.isDebug())
			log.debug("[onDeleteDirectory] " + path);
		try {

			DeleteFolderThread sn = new DeleteFolderThread();
			sn.path = path;
			executor.execute(sn);
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					// log.warn("sync did not finish in 5 seconds. slow io.");
					Dokan.resetTimeout(15000, arg1);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;
		} catch (DokanOperationException e) {
			throw e;
		} catch (Exception e) {
			log.error("unable to delete file " + path, e);
			throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
		}
	}

	@Override
	public void onMoveFile(String from, String to, boolean replaceExisiting,
			DokanFileInfo arg3) throws DokanOperationException {
		if (SDFSLogger.isDebug())
			log.debug("==> [onMoveFile] " + from + " -> " + to
				+ ", replaceExisiting = " + replaceExisiting);

		try {
			DedupFileChannel ch = this.dedupChannels.get(arg3.handle);
			if (ch != null) {
				MoveFileThread sn = new MoveFileThread();
				sn.from = from;
				sn.to = to;
				executor.execute(sn);
				while (!sn.done) {
					synchronized (sn) {
						sn.wait(CHANNEL_TIMEOUT);
					}
					if (!sn.done) {
						// log.warn("sync did not finish in 5 seconds. slow io.");
						Dokan.resetTimeout(15000, arg3);
					}
				}
				if (sn.errRtn != null)
					throw sn.errRtn;
				try {
					ch.getDedupFile().unRegisterChannel(ch, -1);
					this.closeFileChannel(arg3.handle, arg3);
				} catch (Exception e) {
					log.error("unable to close " + from, e);
				}
			}
		} catch (Exception e) {
			log.error("unable to move file " + from + " to " + to, e);
			throw new DokanOperationException(ERROR_FILE_EXISTS);
		}
		// log("<== [onMoveFile]");
	}

	@Override
	public void onLockFile(String fileName, long arg1, long arg2,
			DokanFileInfo arg3) throws DokanOperationException {
		if (SDFSLogger.isDebug())
			log.debug("[onLockFile] " + fileName);
	}

	@Override
	public void onUnlockFile(String fileName, long arg1, long arg2,
			DokanFileInfo arg3) throws DokanOperationException {
		if (SDFSLogger.isDebug())
			log.debug("[onUnlockFile] " + fileName);
	}

	@Override
	public DokanDiskFreeSpace onGetDiskFreeSpace(DokanFileInfo arg0)
			throws DokanOperationException {
		if (SDFSLogger.isDebug())
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
		if (SDFSLogger.isDebug())
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
			File f = resolvePath(path);
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

	private void closeFileChannel(long handleNo, DokanFileInfo info) {
		DedupFileChannel ch = this.dedupChannels.remove(handleNo);
		if (ch != null) {
			try {
				CloseThread cl = new CloseThread();
				cl.ch = ch;
				try {

					executor.execute(cl);
					while (!cl.done) {
						synchronized (cl) {
							cl.wait(CHANNEL_TIMEOUT);
						}
						if (!cl.done && info != null) {
							Dokan.resetTimeout(15000, info);
						}
					}
					if (cl.errRtn != null)
						throw cl.errRtn;
				} catch (RejectedExecutionException e) {
					log.warn("Threads exhausted in close");
				}
				// ch.getDedupFile().unRegisterChannel(ch, -1);
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

	private static File resolvePath(String path) throws DokanOperationException {
		File _f = new File(mountedVolume + path);
		if (!_f.exists()) {
			_f = null;
			log.error("No such node " + path);
			throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		}
		return _f;
	}

	private static class WriteThread implements Runnable {
		public DedupFileChannel ch;
		public long pos;
		public ByteBuffer buf;
		Exception errRtn;
		boolean done;

		@Override
		public void run() {
			if (SDFSLogger.isDebug()) {
				activeTh.incrementAndGet();
				wrTh.incrementAndGet();
			}
			try {
				ch.writeFile(buf, buf.capacity(), 0, pos, true);

			} catch (Exception e) {
				SDFSLogger.getLog().error("error while writing data", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}
			if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(
					"active threads is " + activeTh.decrementAndGet()
							+ " wrTh = " + wrTh.decrementAndGet() + " sync = "
							+ syncTh.get() + " read = " + readTh.get()
							+ " close = " + clTh.get());

		}
	}

	private static class ReadThread implements Runnable {
		public DedupFileChannel ch;
		public long pos;
		public ByteBuffer buf;
		Exception errRtn;
		boolean done;

		@Override
		public void run() {
			if (SDFSLogger.isDebug()) {
				activeTh.incrementAndGet();
				readTh.incrementAndGet();
			}
			try {
				ch.read(buf, 0, buf.capacity(), pos);

			} catch (Exception e) {
				SDFSLogger.getLog().error("error while writing data", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}
			if(SDFSLogger.isDebug())
			SDFSLogger.getLog().debug(
					"active threads is " + activeTh.decrementAndGet()
							+ " wrTh = " + wrTh.get() + " sync = "
							+ syncTh.get() + " read = "
							+ readTh.decrementAndGet() + " close = "
							+ clTh.get());

		}
	}
	
	private static class TruncateThread implements Runnable {
		public DedupFileChannel ch;
		public long sz;
		Exception errRtn;
		boolean done;

		@Override
		public void run() {

			try {
				ch.truncateFile(sz);

			} catch (Exception e) {
				SDFSLogger.getLog().error("error while truncating file", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}
			

		}
	}

	private static class SyncThread implements Runnable {

		public DedupFileChannel ch;
		Exception errRtn;
		boolean done;

		@Override
		public void run() {

			try {
				ch.force(true);

			} catch (Exception e) {
				SDFSLogger.getLog().error("error while writing data", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}
			

		}
	}

	private static class CloseThread implements Runnable {
		public DedupFileChannel ch;
		Exception errRtn;
		boolean done;

		@Override
		public void run() {
			if (SDFSLogger.isDebug()) {
				activeTh.incrementAndGet();
				clTh.incrementAndGet();
			}
			try {
				ch.getDedupFile().unRegisterChannel(ch, -1);

			} catch (Exception e) {
				SDFSLogger.getLog().error("error while writing data", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"active threads is " + activeTh.decrementAndGet()
								+ " wrTh = " + wrTh.get() + " sync = "
								+ syncTh.get() + " read = " + readTh.get()
								+ " close = " + clTh.decrementAndGet());

		}
	}

	private static class ListFiles implements Runnable {
		Win32FindData[] filedata;
		Exception errRtn;
		boolean done;
		String pathName;

		@Override
		public void run() {
			try {
				File f = WinSDFS.resolvePath(pathName);
				File[] mfs = f.listFiles();
				ArrayList<Win32FindData> files = new ArrayList<Win32FindData>();
				for (int i = 0; i < mfs.length; i++) {
					File _mf = mfs[i];
					MetaDataDedupFile mf = MetaFileStore.getMF(_mf.getPath());
					MetaDataFileInfo fi = new MetaDataFileInfo(_mf.getName(),
							mf);
					files.add(fi.toWin32FindData());
				}
				filedata = files.toArray(new Win32FindData[0]);
			} catch (Exception e) {
				SDFSLogger.getLog().error("error while listing files", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}
			// log("[onFindFiles] " + files);

		}

	}

	private static class SetTimeThread implements Runnable {
		String fileName;
		long atime;
		long mtime;
		Exception errRtn;
		boolean done;

		@Override
		public void run() {
			try {
				File f = resolvePath(fileName);
				MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
				mf.setLastAccessed(atime * 1000L, true);
				mf.setLastModified(mtime * 1000L, true);
			} catch (Exception e) {
				SDFSLogger.getLog().error("error while setting time", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}
			// log("[onFindFiles] " + files);

		}

	}

	private static class DeleteFileThread implements Runnable {
		String fileName;
		boolean done;
		Exception errRtn;

		@Override
		public void run() {
			try {
				File f = resolvePath(fileName);

				if (!MetaFileStore.removeMetaFile(f.getPath(), true)) {
					log.warn("unable to delete file " + f.getPath());
					throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("error while deleting file", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}

		}

	}

	private static class DeleteFolderThread implements Runnable {
		String path;
		boolean done;
		Exception errRtn;

		@Override
		public void run() {
			try {
				File f = resolvePath(path);

				if (!MetaFileStore.removeMetaFile(f.getPath(), true)) {
					log.error("unable to delete folder " + f.getPath());
					throw new DokanOperationException(
							WinError.ERROR_DIR_NOT_EMPTY);
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("error while deleting folder", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}

		}

	}

	private static class MoveFileThread implements Runnable {
		String from;
		String to;
		boolean done;
		Exception errRtn;

		@Override
		public void run() {
			try {
				File f = resolvePath(from);
				MetaFileStore.rename(f.getPath(), mountedVolume + to);

			} catch (Exception e) {
				SDFSLogger.getLog().error("error while setting moving file", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}

		}

	}

	private static class MkDirThread implements Runnable {
		String pathName;
		boolean done;
		Exception errRtn;

		@Override
		public void run() {
			try {
				File f = new File(mountedVolume + pathName);
				if (f.exists()) {
					f = null;
					throw new DokanOperationException(
							WinError.ERROR_ALREADY_EXISTS);
				}
				f.mkdir();
			} catch (Exception e) {
				//SDFSLogger.getLog().error("error while setting time", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}

		}

	}
}
