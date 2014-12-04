package org.opendedup.mtools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.DataMapInterface;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.sdfs.servers.SDFSService;
import org.opendedup.util.FileCounts;
import org.opendedup.util.StorageUnit;
import org.opendedup.util.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;

public class FFDisk {
	private AtomicLong files = new AtomicLong(0);
	private AtomicLong lsz = new AtomicLong(0);
	private AtomicLong corruptFiles = new AtomicLong(0);
	public SDFSEvent fEvt = null;
	private static final int MAX_BATCH_SIZE = 10000;
	private boolean failed = false;
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(
			2);
	private transient ThreadPoolExecutor executor = new ThreadPoolExecutor(Main.writeThreads/2,
			Main.writeThreads/2, 10, TimeUnit.SECONDS, worksQueue,new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY),
			executionHandler);
	int fc = 0;
	public FFDisk(String file) throws FDiskException, IOException {
		File f = new File(Main.dedupDBStore);
		long sz = FileCounts.getSize(f, false);
		fc = (int)FileCounts.getCount(f, false);
		fEvt = SDFSEvent.fdiskInfoEvent(
				"Starting FDISK for " + file
						+ " file count = " + fc
						+ " file size = " + StorageUnit.of(sz).format(sz));
		fEvt.maxCt = sz;
		
	}

	public void init(String file) throws FDiskException {
		File f = new File(Main.volume.getPath() + File.separator + file);
		if (!f.exists()) {
			
					fEvt.endEvent(
							"FDisk Will not start because the file " + file + " does not exist");
			throw new FDiskException(
					"FDisk Will not start because the file " + file + " does not exist");
		}
		try {
			
			SDFSLogger.getLog().info(
					"Starting FDISK for " + f.getPath());
			long start = System.currentTimeMillis();

			this.traverse(f);
			executor.shutdown();
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				  SDFSLogger.getLog().debug("Awaiting fdisk completion of threads.");
				}
			if(failed)
				throw new IOException("FDisk traverse failed");
			long dur = (System.currentTimeMillis() - start) / 1000;
			long spd = 0;
			if(dur > 0)
				spd = lsz.get()/dur;
			
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check [" + files + "]. Found ["
							+ this.corruptFiles + "] corrupt files. Speed= "+ StorageUnit.of(spd).format(spd) + "/s");

			fEvt.endEvent("took [" + (System.currentTimeMillis() - start)
					/ 1000 + "] seconds to check [" + files + "]. Found ["
					+ this.corruptFiles + "] corrupt files. Speed= "+ StorageUnit.of(spd).format(spd) + "/s");
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
			fEvt.endEvent("fdisk failed because [" + e.toString() + "]",
					SDFSEvent.ERROR);
			throw new FDiskException(e);

		}
	}
	ArrayList<File> filesAL = new ArrayList<File>();
	private void createFileList(File dir) throws IOException {
		if(SDFSService.isStopped())
			return;
		
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				createFileList(new File(dir, children[i]));
			}
		} else {
			filesAL.add(dir);
		}
		
		
	}
	
	Function<File, Long> getLastModified = new Function<File, Long>() {
	    public Long apply(File file) {
			try {
				MetaDataDedupFile mf = MetaDataDedupFile.getFile(file.getPath());
				return mf.lastModified();
			} catch (Exception e) {
				SDFSLogger.getLog().error("error getting attributes for " + file.getPath(),e);
			}
	    	return 0L;
	    }
	};

	
	
	private void traverse(File dir) throws IOException {
		if(SDFSService.isStopped())
			return;
		filesAL = new ArrayList<File>(fc);
		this.createFileList(dir);
		List<File> orderedFiles = Ordering.natural().onResultOf(getLastModified).
                sortedCopy(filesAL);
		filesAL = null;
		for(File f : orderedFiles) {
			if(failed)
				throw new IOException("FDisk traverse failed");
			if(SDFSService.isStopped())
				return;
			executor.execute(new CheckDedupFile(this,f));
		}
	}

	private int batchCheck(ArrayList<HashLocPair> chunks)
			throws IOException {
		List<HashLocPair> pchunks = HCServiceProxy.batchHashExists(chunks);
		int corruptBlocks = 0;
		for (HashLocPair p : pchunks) {
			byte[] exists = p.hashloc;
			if (exists[0] == -1) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"could not find "
									+ StringUtils.getHexString(p.hash));
				corruptBlocks++;
			}
		}
		return corruptBlocks;
	}
	ReentrantLock l = new ReentrantLock();
	private void checkDedupFile(File metaFile) throws IOException {
		DataMapInterface mp = null;
		MetaDataDedupFile mf = null;
		try {
			long start = System.currentTimeMillis();
			mf = MetaDataDedupFile.getFile(metaFile.getPath());
			lsz.addAndGet(mf.length());
			String guid= mf.getDfGuid();
			File directory = new File(Main.dedupDBStore + File.separator
					+ guid.substring(0, 2) + File.separator
					+ guid);
			File dbf = new File(directory.getPath() + File.separator
					+ guid + ".map");
			mp = new LongByteArrayMap(dbf.getPath());
			long prevpos = 0;
			ArrayList<HashLocPair> chunks = new ArrayList<HashLocPair>(
					MAX_BATCH_SIZE);
			byte[] val = new byte[0];
			mp.iterInit();
			long corruptBlocks = 0;
			while (val != null) {
				l.lock();
				fEvt.curCt += (mp.getIterPos() - prevpos);
				l.unlock();
				prevpos = mp.getIterPos();
				val = mp.nextValue();
				if (val != null) {
					SparseDataChunk ck = new SparseDataChunk(val,mp.getVersion());
						if (Main.chunkStoreLocal) {
							List<HashLocPair> al = ck.getFingers();
							for (HashLocPair p : al) {
								byte[] exists = HCServiceProxy.hashExists(
										p.hash, false,
										Main.volume.getClusterCopies());
								if (exists[0] == -1) {
									if (SDFSLogger.isDebug())
										SDFSLogger
												.getLog()
												.debug("file ["
														+ metaFile.getPath()
														+ "] could not find "
														+ StringUtils
																.getHexString(p.hash));
									corruptBlocks++;
								} else if (SDFSLogger.isDebug()) {
									SDFSLogger
									.getLog()
									.debug("file ["
											+ metaFile.getPath()
											+ "] found "
											+ StringUtils
													.getHexString(p.hash));
									
								}
							}
						} else {
							chunks.addAll(ck.getFingers());
							if (chunks.size() >= MAX_BATCH_SIZE) {
								corruptBlocks += batchCheck(chunks);
								chunks = new ArrayList<HashLocPair>(
										MAX_BATCH_SIZE);
							}
						}
				}
			}
			if (chunks.size() > 0) {
				corruptBlocks += batchCheck(chunks);
			}
			if (corruptBlocks > 0) {
				this.corruptFiles.incrementAndGet();
				SDFSLogger.getLog().warn(
						"map file " + metaFile.getPath()+ " is suspect, ["
								+ corruptBlocks + "] missing blocks found.");
			}
			long dur = (System.currentTimeMillis() - start) / 1000;
			long spd = 0;
			if(dur > 0)
				spd = mf.length()/dur;
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check [" + metaFile.getPath() + "]. Found ["
							+ corruptBlocks + "] corrupt blocks. Speed= "+ StorageUnit.of(spd).format(spd) + "/s");
			
		} catch (Throwable e) {
			SDFSLogger.getLog().error(
					"error while checking file [" + metaFile.getPath() + "]", e);
			// throw new IOException(e);
		} finally {
			mp.close();
			mp = null;
		}
		this.files.incrementAndGet();
	}

	private static class CheckDedupFile implements Runnable {

		FFDisk fd = null;
		File f = null;

		protected CheckDedupFile(FFDisk fd, File f) {
			this.fd = fd;
			this.f = f;
		}

		@Override
		public void run() {
			try {
				fd.checkDedupFile(f);
			} catch (IOException e) {
				SDFSLogger.getLog().error("error running fdisk", e);
			}
		}

	}

	private final static class ProcessPriorityThreadFactory implements
			ThreadFactory {

		private final int threadPriority;

		public ProcessPriorityThreadFactory(int threadPriority) {
			this.threadPriority = threadPriority;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r);
			thread.setPriority(threadPriority);
			return thread;
		}

	}

}
