package org.opendedup.sdfs.windows.fs;

import static net.decasdev.dokan.WinError.ERROR_GEN_FAILURE;

import static net.decasdev.dokan.WinError.ERROR_DISK_FULL;
import static net.decasdev.dokan.WinError.ERROR_FILE_EXISTS;
import static net.decasdev.dokan.WinError.ERROR_FILE_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_PATH_NOT_FOUND;
import static net.decasdev.dokan.WinError.ERROR_READ_FAULT;
import static net.decasdev.dokan.WinError.ERROR_WRITE_FAULT;
//import static net.decasdev.dokan.WinError.ERROR_IO_PENDING;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.decasdev.dokan.ByHandleFileInformation;
import net.decasdev.dokan.CreationDisposition;
import net.decasdev.dokan.Dokan;
import net.decasdev.dokan.DokanDiskFreeSpace;
import net.decasdev.dokan.DokanFileInfo;
import net.decasdev.dokan.DokanOperationException;
import net.decasdev.dokan.DokanOperations;
import net.decasdev.dokan.DokanOptions;
import net.decasdev.dokan.DokanOptionsMode;
import net.decasdev.dokan.DokanVolumeInformation;
import net.decasdev.dokan.FileFlag.FileFlags;
import net.decasdev.dokan.FileTimeUtils;
import net.decasdev.dokan.Win32FindData;
import net.decasdev.dokan.WinError;
import net.decasdev.dokan.FileFlag;

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
	public static final int FILE_DIRECTORY_FILE = 0x00000001;
	public static final int FILE_PERSISTENT_ACLS = 0x00000008;
	public static final int FILE_SUPPORTS_REMOTE_STORAGE = 256;
	public static final int SUPPORTED_FLAGS = FILE_CASE_PRESERVED_NAMES | FILE_SUPPORTS_REMOTE_STORAGE
			| FILE_UNICODE_ON_DISK | FILE_PERSISTENT_ACLS;
	/** Next handle */
	static long nextHandleNo = 1;
	final long rootCreateTime = FileTimeUtils.toFileTime(new Date());
	long rootLastWrite = rootCreateTime;
	private static String mountedVolume = null;
	private static String driveLetter = "S:\\";
	private static Logger log = SDFSLogger.getFSLog();
	static ConcurrentHashMap<Long, DedupFileChannel> dedupChannels = new ConcurrentHashMap<Long, DedupFileChannel>();
	// private static RejectedExecutionHandler executionHandler = new
	// BlockPolicy();
	private static BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	private static ThreadPoolExecutor executor = new ThreadPoolExecutor(1, Main.writeThreads * 8, 10, TimeUnit.SECONDS,
			worksQueue);
	private static final int CHANNEL_TIMEOUT = 1000;
	private static final int RESET_DURATION = 4 * 60 * 1000;

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
		// int version = Dokan.getVersion();
		// System.out.println("version = " + version);
		// int driverVersion = Dokan.getDriverVersion();
		// System.out.println("driverVersion = " + driverVersion);
	}

	void mount(String _driveLetter, String _mountedVolume, boolean debug) {
		if (debug)
			SDFSLogger.setFSLevel(0);
		else
			SDFSLogger.setFSLevel(1);
		mountedVolume = _mountedVolume;
		driveLetter = _driveLetter;
		DokanOptions dokanOptions = new DokanOptions();
		dokanOptions.mountPoint = driveLetter;
		dokanOptions.threadCount = Main.writeThreads;
		dokanOptions.metaFilePath = mountedVolume;
		dokanOptions.optionsMode = DokanOptionsMode.Mode.REMOVABLE_DRIVE.getValue();
		// dokanOptions.uncPath = "\\opendedupe\\awesome";
		log.info("######## mounting " + mountedVolume + " to " + driveLetter + " #############");
		System.out.println("volumemounted");
		System.out.println("");
		int result = Dokan.mount(dokanOptions, this, debug);

		if (result < 0) {
			System.out.println("Unable to mount volume because result = " + result);
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
				System.out.println("Can't assign a drive letter or mount point");
			if (result == -6)
				System.out.println("Mount point is invalid");
			System.exit(-1);

		} else {
			Collection<DedupFileChannel> iter = dedupChannels.values();
			for (DedupFileChannel ch : iter) {
				try {
					ch.getDedupFile().unRegisterChannel(ch, -1);
				} catch (Exception e) {

				}
			}
			log.info("######## unmounted " + mountedVolume + " from " + driveLetter + " #############");
			System.exit(1);
		}
	}

	static synchronized long getNextHandle() {
		if (nextHandleNo == Long.MAX_VALUE)
			nextHandleNo = 0;
		return nextHandleNo++;
	}

	private void checkLocal(String fileName) throws DokanOperationException {
		if(!new File(mountedVolume + fileName).getPath().startsWith(mountedVolume)) {
			log.error("access denied to " +new File(mountedVolume + fileName).getPath());
			throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED); 
		}
	}
	@Override
	public long onCreateFile(String fileName, int desiredAccess, int shareMode, int creationDisposition,
			int flagsAndAttributes, int createOptions, DokanFileInfo fileInfo) throws DokanOperationException {
		try {
			this.checkLocal(fileName);
			CreateFileThread sn = new CreateFileThread(fileName, desiredAccess, shareMode, creationDisposition,
					createOptions, flagsAndAttributes, fileInfo, this);
			int z = 0;
			try {

				executor.execute(sn);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(sn);
				th.start();
			}
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					z++;
					Dokan.resetTimeout(RESET_DURATION, fileInfo);
					log.debug("onCreateFile did not finish in " + (z * CHANNEL_TIMEOUT) / 1000 + " seconds. slow io."
							+ fileInfo.handle);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;
			log.debug("fn=" + fileName + " handle=" + sn.nextHandle);
			return sn.nextHandle;

		} catch (DokanOperationException e) {
			log.debug("dokan error " + fileName, e);
			throw e;
		} catch (Exception e) {
			log.error("unable to create file ", e);
			throw new DokanOperationException(WinError.ERROR_INVALID_FUNCTION);
		}
	}

	@Override
	public void onCleanup(String fileName, DokanFileInfo arg1) throws DokanOperationException {
		if (!fileName.equals("\\")) {
			try {
				if (SDFSLogger.isFSDebug())
					log.debug("[onCleanup] " + fileName);
				SyncThread sn = new SyncThread();
				sn.fileName = fileName;
				sn.info = arg1;
				int z = 0;
				try {

					executor.execute(sn);
				} catch (RejectedExecutionException e) {
					log.warn("Threads exhausted");
					Thread th = new Thread(sn);
					th.start();
				}
				while (!sn.done) {
					synchronized (sn) {
						sn.wait(CHANNEL_TIMEOUT);
					}
					if (!sn.done) {
						z++;
						Dokan.resetTimeout(RESET_DURATION, arg1);
						log.debug("sync did not finish in " + (z * CHANNEL_TIMEOUT) / 1000 + " seconds. slow io."
								+ arg1.handle);
					}
				}
				if (sn.errRtn != null)
					throw sn.errRtn;
				
				this.onCloseFile(fileName, arg1);
			} catch (DokanOperationException e) {
				throw e;
			} catch (Exception e) {
				log.error("unable to cleanup file " + fileName, e);
				throw new DokanOperationException(ERROR_WRITE_FAULT);
			}
		}
	}

	@Override
	public void onCloseFile(String path, DokanFileInfo arg1) throws DokanOperationException {
		if (!path.equals("\\")) {
			try {
				if (SDFSLogger.isFSDebug())
					log.debug("[onClose] " + path);
				this.closeFileChannel(arg1.handle, arg1);
				if (arg1.deleteOnClose) {
					this.onDeleteFile(path, arg1);
				}
			} catch (DokanOperationException e) {
				throw e;
			} catch (Exception e) {
				log.error("unable to close file " + path, e);
			}
		}
	}

	@Override
	public int onReadFile(String fileName, ByteBuffer buf, long offset, DokanFileInfo arg3)
			throws DokanOperationException {

		try {
			if (SDFSLogger.isFSDebug())
				log.debug("[onReadFile] " + fileName + " " + offset + " " + buf.capacity() + " " + buf.position());
			ReadThread wr = new ReadThread();
			wr.buf = buf;
			wr.info = arg3;
			wr.fileName = fileName;
			wr.pos = offset;
			int z = 0;
			try {

				executor.execute(wr);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(wr);
				th.start();
			}
			while (!wr.done) {
				synchronized (wr) {
					wr.wait(CHANNEL_TIMEOUT);
				}

				if (!wr.done) {
					z++;
					log.debug("write did not finish in " + (z * CHANNEL_TIMEOUT) / 1000 + "seconds. slow io."
							+ arg3.handle);
					Dokan.resetTimeout(RESET_DURATION, arg3);
				}
			}
			if (wr.errRtn != null)
				throw wr.errRtn;

			// int read = ch.read(buf, 0, buf.capacity(), offset);
			// if (read == -1)
			// read = 0;
			return wr.read;
		} catch(NullPointerException e) {
			log.debug("unable to read file " + fileName, e);
			throw new DokanOperationException(ERROR_READ_FAULT);
		}catch (Exception e) {
			log.error("unable to read file " + fileName, e);
			throw new DokanOperationException(ERROR_READ_FAULT);
		}
	}

	@Override
	public int onWriteFile(String fileName, ByteBuffer buf, long offset, DokanFileInfo arg3)
			throws DokanOperationException {
		try {
			if (Main.volume.isFull())
				throw new DokanOperationException(ERROR_DISK_FULL);
			if (SDFSLogger.isFSDebug())
				log.debug("[onWriteFile] " + fileName + " sz=" + buf.capacity() + " offset=" + offset);

			/*
			 * WriteThread th = new WriteThread(); th.buf = buf; th.offset =
			 * offset; th.ch = ch; executor.execute(th);
			 */
			WriteThread wr = new WriteThread();
			wr.buf = buf;
			wr.fileName = fileName;
			wr.info = arg3;
			wr.pos = offset;
			int z = 0;
			try {

				executor.execute(wr);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(wr);
				th.start();
			}
			while (!wr.done) {
				synchronized (wr) {
					wr.wait(CHANNEL_TIMEOUT);
				}
				if (!wr.done) {
					z++;
					log.debug("write did not finish in " + (z * CHANNEL_TIMEOUT) / 1000 + " seconds. slow io. ct="
							+ arg3.handle);
					Dokan.resetTimeout(RESET_DURATION, arg3);
				}
			}
			if (wr.errRtn != null)
				throw wr.errRtn;

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
	public void onSetEndOfFile(String fileName, long length, DokanFileInfo arg2) throws DokanOperationException {

		try {
			if (SDFSLogger.isFSDebug())
				log.debug("[onSetEndOfFile] " + fileName);
			truncateFile(fileName, length, arg2);
		} catch (Exception e) {
			log.error("Unable to set length  of " + fileName + " to " + length);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public void onFlushFileBuffers(String fileName, DokanFileInfo arg1) throws DokanOperationException {

		try {
			if (SDFSLogger.isFSDebug())
				log.debug("[onFlushFileBuffers] " + fileName);
			SyncThread sn = new SyncThread();
			sn.fileName = fileName;
			sn.info = arg1;
			int z = 0;
			try {

				executor.execute(sn);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(sn);
				th.start();
			}
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					z++;
					log.debug("fsync did not finish in " + (z * CHANNEL_TIMEOUT) / 1000 + " seconds. slow io."
							+ arg1.handle);
					Dokan.resetTimeout(RESET_DURATION, arg1);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;

		} catch (Exception e) {

			log.error("unable to sync file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}

	}

	@Override
	public ByHandleFileInformation onGetFileInformation(String fileName, DokanFileInfo arg1)
			throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onGetFileInformation] " + fileName);
		try {
			GetFileInfoThread sn = new GetFileInfoThread();
			sn.fileName = fileName;
			int z = 0;
			try {

				executor.execute(sn);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(sn);
				th.start();
			}
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					z++;
					log.debug("ByHandleFileInformation did not finish in " + (z * CHANNEL_TIMEOUT) / 1000
							+ " seconds. slow io." + arg1.handle);
					Dokan.resetTimeout(RESET_DURATION, arg1);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;
			else
				return sn.info;

		} catch (Exception e) {
			log.error("unable to get file info " + fileName, e);
			throw new DokanOperationException(ERROR_GEN_FAILURE);
		}
	}

	@Override
	public Win32FindData[] onFindFiles(String pathName, DokanFileInfo arg1) throws DokanOperationException {
		try {
			if (SDFSLogger.isFSDebug())
				log.debug("[onFindFiles] " + pathName);
			ListFiles sn = new ListFiles();
			sn.pathName = pathName;
			try {

				executor.execute(sn);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(sn);
				th.start();
			}
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					log.debug("find files did not finish in 5 seconds. slow io." + arg1.handle);
					Dokan.resetTimeout(RESET_DURATION, arg1);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;
			else
				return sn.filedata;

		} catch (Exception e) {

			log.error("unable to list file " + pathName, e);
			throw new DokanOperationException(WinError.ERROR_OPEN_FAILED);
		}
	}

	@Override
	public Win32FindData[] onFindFilesWithPattern(String pathName, String arg1, DokanFileInfo arg2)
			throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onFindFilesWithPattern] " + pathName);
		return null;
	}

	@Override
	public void onSetFileAttributes(String fileName, int fileAttributes, DokanFileInfo arg2)
			throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onSetFileAttributes] " + fileName);
		/*
		 * MemFileInfo fi = fileInfoMap.get(fileName); if (fi == null) throw new
		 * DokanOperationException(ERROR_FILE_NOT_FOUND); fi.fileAttribute =
		 * fileAttributes;
		 */
	}

	@Override
	public void onSetFileTime(String fileName, long creationTime, long atime, long mtime, DokanFileInfo arg4)
			throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onSetFileTime] " + fileName);

		try {
			SetTimeThread sn = new SetTimeThread();
			sn.fileName = fileName;
			sn.atime = atime;
			sn.mtime = mtime;
			try {

				executor.execute(sn);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(sn);
				th.start();
			}
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					log.debug("onSetFileTime did not finish in 5 seconds. slow io." + arg4.handle);
					Dokan.resetTimeout(RESET_DURATION, arg4);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;

		} catch (DokanOperationException e) {
			log.error("unable to set file " + fileName, e);
			throw e;
		} catch (Exception e) {
			log.error("unable to set file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	private void truncateFile(String fileName, long sz, DokanFileInfo nfo) throws Exception {
		try {
			TruncateThread wr = new TruncateThread();
			wr.fileName = fileName;
			wr.info = nfo;
			wr.sz = sz;
			try {

				executor.execute(wr);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(wr);
				th.start();
			}
			while (!wr.done) {
				synchronized (wr) {
					wr.wait(CHANNEL_TIMEOUT);
				}
				if (!wr.done) {
					log.debug("truncate did not finish in 5 seconds. slow io." + nfo.handle);
				}
			}
			if (wr.errRtn != null)
				throw wr.errRtn;
		} catch (DokanOperationException e) {
			log.error("unable to truncate file " + fileName, e);
			throw e;
		} catch (Exception e) {
			log.error("unable to truncate file " + fileName, e);
			throw new DokanOperationException(ERROR_WRITE_FAULT);
		}
	}

	@Override
	public void onDeleteFile(String fileName, DokanFileInfo arg1) throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onDeleteFile] " + fileName);
		try {

			DeleteFileThread sn = new DeleteFileThread();
			sn.fileName = fileName;
			sn.arg1 = arg1;
			sn.sdfs = this;
			try {

				executor.execute(sn);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(sn);
				th.start();
			}
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					log.debug("onDeleteFile did not finish in 5 seconds. slow io." + arg1.handle);
					Dokan.resetTimeout(RESET_DURATION, arg1);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;
		} catch (DokanOperationException e) {
			log.error("unable to delete file " + fileName, e);
			throw e;
		} catch (Exception e) {
			log.error("unable to delete file " + fileName, e);
			throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
		}
	}

	@Override
	public void onDeleteDirectory(String path, DokanFileInfo arg1) throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onDeleteDirectory] " + path);
		try {

			DeleteFolderThread sn = new DeleteFolderThread();
			sn.path = path;
			try {

				executor.execute(sn);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(sn);
				th.start();
			}
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					log.debug("onDeleteDirectory did not finish in 5 seconds. slow io." + arg1.handle);
					Dokan.resetTimeout(RESET_DURATION, arg1);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;
		} catch (DokanOperationException e) {
			log.error("unable to delete directory " + path, e);
			throw e;
		} catch (Exception e) {
			log.error("unable to delete file " + path, e);
			throw new DokanOperationException(WinError.ERROR_ACCESS_DENIED);
		}
	}

	@Override
	public void onMoveFile(String from, String to, boolean replaceExisiting, DokanFileInfo arg3)
			throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("==> [onMoveFile] " + from + " -> " + to + ", replaceExisiting = " + replaceExisiting);
		if (arg3 != null) {
			log.debug("dokanfileinfo " + arg3.handle);
		}
		try {
			MoveFileThread sn = new MoveFileThread();
			sn.from = from;
			sn.to = to;
			try {

				executor.execute(sn);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(sn);
				th.start();
			}
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					log.debug("sync did not finish in 5 seconds. slow io." + arg3.handle);
					Dokan.resetTimeout(RESET_DURATION, arg3);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;
		} catch (Exception e) {
			log.error("unable to move file " + from + " to " + to, e);
			throw new DokanOperationException(ERROR_FILE_EXISTS);
		}
		// log("<== [onMoveFile]");
	}

	@Override
	public void onLockFile(String fileName, long arg1, long arg2, DokanFileInfo arg3) throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onLockFile] " + fileName);
	}

	@Override
	public void onUnlockFile(String fileName, long arg1, long arg2, DokanFileInfo arg3) throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onUnlockFile] " + fileName);
	}

	@Override
	public DokanDiskFreeSpace onGetDiskFreeSpace(DokanFileInfo arg0) throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onGetDiskFreeSpace]");
		try {
			DokanDiskFreeSpaceThread sn = new DokanDiskFreeSpaceThread();
			try {

				executor.execute(sn);
			} catch (RejectedExecutionException e) {
				log.warn("Threads exhausted");
				Thread th = new Thread(sn);
				th.start();
			}
			while (!sn.done) {
				synchronized (sn) {
					sn.wait(CHANNEL_TIMEOUT);
				}
				if (!sn.done) {
					log.debug("onGetDiskFreeSpace did not finish in 5 seconds. slow io." + arg0.handle);
					Dokan.resetTimeout(RESET_DURATION, arg0);
				}
			}
			if (sn.errRtn != null)
				throw sn.errRtn;
			else
				return sn.info;
		} catch (Exception e) {
			log.error("error getting free disk space", e);
			throw new DokanOperationException(ERROR_GEN_FAILURE);
		}
	}

	@Override
	public DokanVolumeInformation onGetVolumeInformation(String arg0, DokanFileInfo arg1)
			throws DokanOperationException {
		if (SDFSLogger.isFSDebug())
			log.debug("[onGetVolumeInformation]");
		try {
			DokanVolumeInformation info = new DokanVolumeInformation();
			info.fileSystemFlags = SUPPORTED_FLAGS;
			info.maximumComponentLength = 256;
			info.volumeName = "Dedup Filesystem";
			info.fileSystemName = "SDFS";
			info.volumeSerialNumber = (int) (Main.volume.getSerialNumber() >> 32);
			return info;
		} catch (Exception e) {
			log.error("error getting volume info", e);
			throw new DokanOperationException(ERROR_GEN_FAILURE);
		}
	}

	@Override
	public void onUnmount(DokanFileInfo arg0) throws DokanOperationException {
		log.debug("[onUnmount]");
		try {
			Dokan.removeMountPoint(driveLetter);
		} catch (Exception e) {
		}
		Collection<DedupFileChannel> iter = dedupChannels.values();
		for (DedupFileChannel ch : iter) {
			try {
				ch.getDedupFile().unRegisterChannel(ch, -1);
			} catch (Exception e) {

			}
		}
		System.exit(0);
	}

	protected static DedupFileChannel getFileChannel(String path, long handleNo) throws DokanOperationException {
		Long k = new Long(handleNo);
		
		DedupFileChannel ch = dedupChannels.get(k);
		if (ch == null) {
			File f = resolvePath(path);
			if(f.isFile()) {
			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
				ch = mf.getDedupFile(false).getChannel(-1);
				if (dedupChannels.containsKey(k)) {
					ch.getDedupFile().unRegisterChannel(ch, -1);
					ch = dedupChannels.get(k);
				} else {
					dedupChannels.put(k, ch);
					SDFSLogger.getLog().debug("added channel " + k + " for " +path + " hno " + handleNo);
				}
			} catch (Exception e) {
				log.error("unable to open file" + f.getPath(), e);
				throw new DokanOperationException(WinError.ERROR_GEN_FAILURE);
			}
			}else {
				return null;
			}
		}
		return ch;
	}

	protected void closeFileChannel(long handleNo, DokanFileInfo info) {

		try {
			CloseThread cl = new CloseThread();
			cl.handleNo = handleNo;
			try {

				executor.execute(cl);
				int z = 0;
				while (!cl.done) {
					synchronized (cl) {
						cl.wait(CHANNEL_TIMEOUT);
					}
					if (!cl.done && info != null) {
						z++;
						log.debug("waiting for close for " + (z * CHANNEL_TIMEOUT) / 1000 + " " + handleNo);
						Dokan.resetTimeout(RESET_DURATION, info);
					} else {
						return;
					}
				}
				if (cl.errRtn != null)
					throw cl.errRtn;
			} catch (RejectedExecutionException e) {
				Thread th = new Thread(cl);
				th.start();
				log.warn("Threads exhausted in close");
			}
			// ch.getDedupFile().unRegisterChannel(ch, -1);
		} catch (Exception e) {
			log.error("unable to close channel" + handleNo, e);
		} finally {
			log.debug("number of channels is " + dedupChannels.size());
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
			log.debug("No such node " + path);
			throw new DokanOperationException(WinError.ERROR_FILE_NOT_FOUND);
		}
		return _f;
	}

	private static class WriteThread implements Runnable {
		public DokanFileInfo info;
		public String fileName;
		public long pos;
		public ByteBuffer buf;
		Exception errRtn;
		boolean done;

		@Override
		public void run() {
			try {
				DedupFileChannel ch = getFileChannel(fileName, info.handle);
				if (info.writeToEndOfFile) {
					log.debug("writing to end of file" + ch.getFile().length());
					pos = ch.getFile().length();
				}
				ch.writeFile(buf, buf.capacity(), 0, pos, true);

				/*
				 * ch.force(true); byte [] z = new byte [buf.capacity()] ; byte
				 * [] r = new byte [buf.capacity()]; buf.position(0);
				 * buf.get(z); ch.read(ByteBuffer.wrap(r), 0, r.length, pos);
				 * int i = hf.hashBytes(z).asInt(); int o =
				 * hf.hashBytes(r).asInt(); if(i != o) { log.warn("i=" +i +
				 * " o=" +o + " pos=" + pos); }
				 */

			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while writing data", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
			}

		}
	}

	private static class ReadThread implements Runnable {
		public DokanFileInfo info;
		public String fileName;
		public long pos;
		public ByteBuffer buf;
		Exception errRtn;
		boolean done;
		public int read = 0;

		@Override
		public void run() {
			try {
				DedupFileChannel ch = getFileChannel(fileName, info.handle);
				ch.read(buf, 0, buf.capacity(), pos);
				read = buf.position();
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while reading data", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
			}

		}
	}

	private static class TruncateThread implements Runnable {

		public long sz;
		Exception errRtn;
		boolean done;
		public DokanFileInfo info;
		public String fileName;

		@Override
		public void run() {
			try {
				DedupFileChannel ch = getFileChannel(fileName, info.handle);
				ch.truncateFile(sz);

			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while truncating file", e);
				errRtn = e;
			}
			done = true;
			synchronized (this) {
				this.notifyAll();
			}

		}
	}

	private static class SyncThread implements Runnable {

		public DokanFileInfo info;
		public String fileName;
		Exception errRtn;
		boolean done;

		@Override
		public void run() {
			
			try {
					DedupFileChannel ch = getFileChannel(fileName, info.handle);
					if(ch != null)
						ch.force(true);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while sync data", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
			}

		}
	}

	private static class CloseThread implements Runnable {
		long handleNo;
		Exception errRtn;
		boolean done;

		@Override
		public void run() {
			try {
				Long k = new Long(handleNo);
				DedupFileChannel ch = dedupChannels.remove(k);
				if (ch != null) {
					ch.getDedupFile().unRegisterChannel(ch, -1);
					if (ch.getFile() != null && ch.getFile().deleteOnClose) {
						MetaFileStore.removeMetaFile(ch.getFile().getPath(), true, true,true);
						log.debug("Deleted file on close");
					}
				}
				
				//SDFSLogger.getLog().info("open channel size is " + dedupChannels.size());
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while reading data", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
			}
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
				//SDFSLogger.getLog().info("Listing " + f.getPath() +  " sz=" +f.listFiles().length);
				
				
				if (!f.exists())
					throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
				ArrayList<Win32FindData> al = new ArrayList<Win32FindData>();
				Path dir = FileSystems.getDefault().getPath( f.getPath() );
			    DirectoryStream<Path> stream = Files.newDirectoryStream( dir );
			      for (Path p : stream) {
			    	  File _mf = p.toFile();
			    	  try {
			    		  MetaDataDedupFile mf = MetaFileStore.getNCMF(new File(_mf.getPath()));
						MetaDataFileInfo fi = new MetaDataFileInfo(_mf.getName(), mf);
						log.debug(fi.toString());
						al.add(fi.toWin32FindData());
			    	  }catch(Exception e) {
			    		  SDFSLogger.getLog().error("error getting file " + _mf.getPath(),e);
			    	  }
			      }
			      stream.close();
			      filedata = al.toArray(new Win32FindData[al.size()]);
			} catch (Exception e) {
				SDFSLogger.getLog().error("error while listing files", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
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
				// SDFSLogger.getLog().info("set "+fileName +" mtime=" + mtime +
				// " atime=" + atime);
				File f = resolvePath(fileName);
				MetaDataDedupFile mf = MetaFileStore.getMF(f.getPath());
				mf.setLastAccessed(MetaDataFileInfo.filetimeToMillis(atime), true);
				mf.setLastModified(MetaDataFileInfo.filetimeToMillis(mtime), true);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while setting time", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
			}
			// log("[onFindFiles] " + files);

		}

	}

	private static class DeleteFileThread implements Runnable {
		String fileName;
		boolean done;
		Exception errRtn;
		WinSDFS sdfs;
		DokanFileInfo arg1;

		@Override
		public void run() {
			try {
				DedupFileChannel ch = WinSDFS.getFileChannel(fileName, arg1.handle);
				if (ch != null) {
					try {
						sdfs.closeFileChannel(arg1.handle, arg1);
					} catch (Exception e) {
						log.error("unable to close " + fileName, e);
					}
				}
				File f = resolvePath(fileName);

				if (!MetaFileStore.removeMetaFile(f.getPath(), true, false,true)) {
					log.warn("unable to delete file " + f.getPath());
					throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
				}
				log.debug("deleteted file " + fileName);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while deleting file", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
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

				if (!MetaFileStore.removeMetaFile(f.getPath(), true, false,true)) {
					log.error("unable to delete folder " + f.getPath());
					throw new DokanOperationException(WinError.ERROR_DIR_NOT_EMPTY);
				}
				log.debug("deleteted folder " + path);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while deleting folder", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
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
				log.debug("moved " + from + " to " + to);
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while setting moving file", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
			}

		}

	}

	private static class GetFileInfoThread implements Runnable {
		String fileName;
		ByHandleFileInformation info;
		boolean done;
		Exception errRtn;

		@Override
		public void run() {
			try {
				MetaDataDedupFile mf = MetaFileStore.getMF(resolvePath(fileName).getPath());
				MetaDataFileInfo fi = new MetaDataFileInfo(fileName, mf);
				info = fi.toByHandleFileInformation();
				log.debug(fi.toString());
			} catch (Exception e) {
				SDFSLogger.getLog().debug("error while setting fileing file", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
			}

		}

	}

	private static class DokanDiskFreeSpaceThread implements Runnable {
		DokanDiskFreeSpace info;
		boolean done;
		Exception errRtn;

		@Override
		public void run() {
			try {
				DokanDiskFreeSpace free = new DokanDiskFreeSpace();
				free.freeBytesAvailable = Main.volume.getCapacity() - Main.volume.getCurrentSize();
				free.totalNumberOfBytes = Main.volume.getCapacity();
				free.totalNumberOfFreeBytes = Main.volume.getCapacity() - Main.volume.getCurrentSize();
				this.info = free;
			} catch (Exception e) {
				log.error("error getting free disk space", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
			}

		}

	}

	private static class CreateFileThread implements Runnable {

		boolean done;
		Exception errRtn;
		String fileName;
		int desiredAccess;
		int shareMode;
		int creationDisposition;
		int flagsAndAttributes;
		int createOptions;
		long nextHandle = -1;
		DokanFileInfo arg5;
		WinSDFS fs;

		public CreateFileThread(String fileName, int desiredAccess, int shareMode, int creationDisposition,
				int flagsAndAttributes, int createOptions, DokanFileInfo arg5, WinSDFS fs) {
			this.fileName = fileName;
			this.desiredAccess = desiredAccess;
			this.shareMode = shareMode;
			this.creationDisposition = creationDisposition;
			this.flagsAndAttributes = flagsAndAttributes;
			this.createOptions = createOptions;
			this.fs = fs;
			this.arg5 = arg5;
		}

		@Override
		public void run() {
			try {
				if (SDFSLogger.isFSDebug())
					log.debug("[onCreateFile] ");
				CreationDisposition disposition = CreationDisposition.build(creationDisposition);
				if (SDFSLogger.isFSDebug())
					log.debug("[onCreateFile] " + fileName + ", creationDisposition = " + disposition + " shareMode="
							+ shareMode + " desiredAccess=" + desiredAccess + " flagsAndAttributes="
							+ flagsAndAttributes + " createOptions=" + createOptions + " filePath="
							+ new File(mountedVolume + fileName).getPath() + " exists="
							+ new File(mountedVolume + fileName).exists());
				EnumSet<FileFlags> flags = FileFlag.getFlags(flagsAndAttributes);
				if ((createOptions & FILE_DIRECTORY_FILE) == FILE_DIRECTORY_FILE
						|| flags.contains(FileFlags.FILE_DIRECTORY_FILE)) {
					log.debug("in directory");
					File _f;
					switch (disposition) {
					case FILE_CREATE:
						if (SDFSLogger.isFSDebug())
							log.debug("[onCreateFile] directory_create " + fileName);
						File f = new File(mountedVolume + fileName);
						if (f.exists()) {
							f = null;
							throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
						}
						f.mkdir();
						nextHandle = getNextHandle();
						break;
					case FILE_OPEN:
						if (SDFSLogger.isFSDebug())
							log.debug("[onCreateFile] directory_open " + fileName + " sm="
									+ flags.contains(FileFlags.FILE_FLAG_BACKUP_SEMANTICS));
						if (fileName.equals("\\"))
							nextHandle = getNextHandle();
						fileName = Utils.trimTailBackSlash(fileName);
						_f = new File(mountedVolume + fileName);
						if (_f.exists() && _f.isDirectory()) {
							nextHandle = getNextHandle();
						} else
							throw new DokanOperationException(ERROR_PATH_NOT_FOUND);
						arg5.handle = nextHandle;
						break;
					case FILE_OPEN_IF:
						try {
							if (SDFSLogger.isFSDebug())
								log.debug("[onCreateFile] directory_open_if " + fileName);
							if (fileName.equals("\\"))
								nextHandle = getNextHandle();
							fileName = Utils.trimTailBackSlash(fileName);
							_f = new File(mountedVolume + fileName);
							if (_f.exists() && _f.isDirectory()) {
								nextHandle = getNextHandle();
							} else if (!_f.exists()) {
								_f.mkdir();
								nextHandle = getNextHandle();
							} else
								throw new DokanOperationException(ERROR_PATH_NOT_FOUND);
						} catch (DokanOperationException e) {
							log.debug("dokan error", e);
							throw e;
						} catch (Exception e) {
							log.error("unable to create directory", e);
							throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
						}
						arg5.handle = nextHandle;
						break;
					default:
						log.error("wring disposition " + disposition);
						throw new DokanOperationException(WinError.ERROR_BAD_ARGUMENTS);

					}
				} else {
					log.debug("in file");
					boolean deleteOnClose = false;
					if (flags.contains(FileFlags.FILE_DELETE_ON_CLOSE)) {
						deleteOnClose = true;
					}
					if (SDFSLogger.isFSDebug()) {
						for (FileFlags f : flags) {
							log.debug("flag = " + f.name());
						}
					}
					if (fileName.equals("\\")) {
						log.debug("\\FILE_CREATE");
						switch (disposition) {
						case FILE_CREATE:
							throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
						case FILE_SUPERSEDE:
							throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
						case FILE_OPEN:
							nextHandle = getNextHandle();
							arg5.handle = nextHandle;
							break;
						case FILE_OPEN_IF:
							nextHandle = getNextHandle();
							arg5.handle = nextHandle;
							break;
						case FILE_OVERWRITE:
							throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
						case FILE_OVERWRITE_IF:
							throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
						case UNDEFINED:
							assert (false);

						}
					} else if (new File(mountedVolume + fileName).exists()) {
						switch (disposition) {
						case FILE_CREATE:
							log.debug("\\FILE_CREATE");
							throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
						case FILE_OPEN_IF:
							nextHandle = getNextHandle();
							arg5.handle = nextHandle;
							if (deleteOnClose) {
								MetaDataDedupFile mf = MetaFileStore.getMF(mountedVolume + fileName);
								mf.deleteOnClose = deleteOnClose;
							}
							arg5.handle = nextHandle;
							break;
						case FILE_OPEN:
							log.debug("\\FILE_OPEN");
							nextHandle = getNextHandle();
							arg5.handle = nextHandle;
							if (deleteOnClose) {
								MetaDataDedupFile mf = MetaFileStore.getMF(mountedVolume + fileName);
								mf.deleteOnClose = deleteOnClose;

							}
							break;
						case FILE_SUPERSEDE:
							try {
								log.debug("\\FILE_SUPERSEDE");
								nextHandle = getNextHandle();
								arg5.handle = nextHandle;
								fs.truncateFile(fileName, 0, arg5);
								fs.closeFileChannel(nextHandle, arg5);
								if (deleteOnClose) {
									MetaDataDedupFile mf = MetaFileStore.getMF(mountedVolume + fileName);
									mf.deleteOnClose = deleteOnClose;
								}
							} catch (IOException e) {
								log.error("unable to clear file " + resolvePath(fileName).getPath(), e);
								throw new DokanOperationException(WinError.ERROR_WRITE_FAULT);
							}
							break;
						case FILE_OVERWRITE:
							log.debug("\\FILE_OVERWRITE");
							throw new DokanOperationException(WinError.ERROR_ALREADY_EXISTS);
						case UNDEFINED:
							log.debug("\\UNDEFINED");
							assert (false);
						case FILE_OVERWRITE_IF:
							try {
								log.debug("\\FILE_OVERWRITE_IF");
								nextHandle = getNextHandle();
								arg5.handle = nextHandle;
								fs.truncateFile(fileName, 0, arg5);
								fs.closeFileChannel(nextHandle, arg5);
								if (deleteOnClose) {
									MetaDataDedupFile mf = MetaFileStore.getMF(mountedVolume + fileName);
									mf.deleteOnClose = deleteOnClose;
								}
							} catch (IOException e) {
								log.error("unable to clear file " + resolvePath(fileName).getPath(), e);
								throw new DokanOperationException(WinError.ERROR_WRITE_FAULT);
							}
							break;
						default:
							break;
						}
					} else {
						String path = mountedVolume + fileName;
						switch (disposition) {

						case FILE_CREATE:
							log.debug("FILE_CREATE");
							if (Main.volume.isFull())
								throw new DokanOperationException(ERROR_DISK_FULL);
							try {

								if (SDFSLogger.isFSDebug())
									log.debug("creating " + fileName);
								MetaDataDedupFile mf = MetaFileStore.getMF(path);
								mf.sync(true);
								mf.deleteOnClose = deleteOnClose;

							} catch (Exception e) {
								log.error("unable to create file " + path, e);
								throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
							}
							nextHandle = getNextHandle();
							arg5.handle = nextHandle;
							break;
						case FILE_OVERWRITE_IF:
							log.debug("FILE_OVERWRITE_IF");
							if (Main.volume.isFull())
								throw new DokanOperationException(ERROR_DISK_FULL);
							try {

								if (SDFSLogger.isFSDebug())
									log.debug("creating " + fileName);
								MetaDataDedupFile mf = MetaFileStore.getMF(path);
								mf.sync(true);
								mf.deleteOnClose = deleteOnClose;
							} catch (Exception e) {
								log.error("unable to create file " + path, e);
								throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
							}
							nextHandle = getNextHandle();
							arg5.handle = nextHandle;
							break;
						case FILE_OPEN_IF:
							log.debug("FILE_OPEN_IF");
							if (Main.volume.isFull())
								throw new DokanOperationException(ERROR_DISK_FULL);
							try {
								if (SDFSLogger.isFSDebug())
									log.debug("creating " + fileName);
								MetaDataDedupFile mf = MetaFileStore.getMF(path);
								mf.sync(true);
								mf.deleteOnClose = deleteOnClose;
							} catch (Exception e) {
								log.error("unable to create file " + path, e);
								throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
							}
							nextHandle = getNextHandle();
							arg5.handle = nextHandle;
							break;
						case FILE_OPEN:
							log.debug("FILE_OPEN");
							if (SDFSLogger.isFSDebug())
								log.debug("unable to open file " + path);
							throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
						case FILE_OVERWRITE:
							throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
						case UNDEFINED:
							log.debug("UNDEFINED");
							assert (false);
						case FILE_SUPERSEDE:
							log.debug("FILE_SUPERSEDE");
							if (Main.volume.isFull())
								throw new DokanOperationException(ERROR_DISK_FULL);
							try {
								if (SDFSLogger.isFSDebug())
									log.debug("creating " + fileName);
								MetaDataDedupFile mf = MetaFileStore.getMF(path);
								mf.sync(true);
								mf.deleteOnClose = deleteOnClose;
							} catch (Exception e) {
								log.error("unable to create file " + path, e);
								throw new DokanOperationException(ERROR_FILE_NOT_FOUND);
							}
							nextHandle = getNextHandle();
							arg5.handle = nextHandle;
							break;
						default:
							log.debug("hit default");
							break;
						}
					}
					if (nextHandle < 0)
						throw new DokanOperationException(WinError.ERROR_INVALID_FUNCTION);
				}
			} catch (Exception e) {
				log.debug("error", e);
				errRtn = e;
			} finally {
				done = true;
				synchronized (this) {
					this.notifyAll();
				}
			}

		}

	}
}