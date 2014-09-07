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

import org.opendedup.collections.DataMapInterface;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.io.SparseDataChunk.HashLocPair;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;
import org.opendedup.util.StringUtils;

public class FDisk {
	private AtomicLong files = new AtomicLong(0);
	private AtomicLong corruptFiles = new AtomicLong(0);
	public SDFSEvent fEvt = null;
	private static final int MAX_BATCH_SIZE = 10000;
	private boolean failed = false;
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(
			2);
	private transient ThreadPoolExecutor executor = new ThreadPoolExecutor(Main.writeThreads + 1,
			Main.writeThreads + 1, 10, TimeUnit.SECONDS, worksQueue,new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY),
			executionHandler);
	
	public FDisk(SDFSEvent evt) throws FDiskException {
		init(evt);
	}

	public void init(SDFSEvent evt) throws FDiskException {
		File f = new File(Main.dedupDBStore);
		if (!f.exists()) {
			SDFSEvent
					.fdiskInfoEvent(
							"FDisk Will not start because the volume has not been written too",
							evt)
					.endEvent(
							"FDisk Will not start because the volume has not been written too");
			throw new FDiskException(
					"FDisk Will not start because the volume has not been written too");
		}
		try {
			fEvt = SDFSEvent.fdiskInfoEvent(
					"Starting FDISK for " + Main.volume.getName()
							+ " file count = " + FileCounts.getCount(f, false)
							+ " file size = " + FileCounts.getSize(f, false),
					evt);
			fEvt.maxCt = FileCounts.getSize(f, false);
			SDFSLogger.getLog().info(
					"Starting FDISK for " + Main.volume.getName());
			long start = System.currentTimeMillis();

			this.traverse(f);
			executor.shutdown();
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				  SDFSLogger.getLog().debug("Awaiting fdisk completion of threads.");
				}
			if(failed)
				throw new IOException("FDisk traverse failed");
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check [" + files + "]. Found ["
							+ this.corruptFiles + "] corrupt files");

			fEvt.endEvent("took [" + (System.currentTimeMillis() - start)
					/ 1000 + "] seconds to check [" + files + "]. Found ["
					+ this.corruptFiles + "] corrupt files");
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
			fEvt.endEvent("fdisk failed because [" + e.toString() + "]",
					SDFSEvent.ERROR);
			throw new FDiskException(e);

		}
	}

	private void traverse(File dir) throws IOException {
		if (dir.isDirectory()) {
			if(failed)
				throw new IOException("FDisk traverse failed");
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				traverse(new File(dir, children[i]));
			}
		} else {
			if(failed)
				throw new IOException("FDisk traverse failed");
			if (dir.getPath().endsWith(".map")) {
				executor.execute(new CheckDedupFile(this,dir));
			}
		}
	}

	private int batchCheck(ArrayList<SparseDataChunk> chunks)
			throws IOException {
		List<SparseDataChunk> pchunks = HCServiceProxy.batchHashExists(chunks);
		int corruptBlocks = 0;
		for (SparseDataChunk ck : pchunks) {
			byte[] exists = ck.getHashLoc();
			if (exists[0] == -1) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"could not find "
									+ StringUtils.getHexString(ck.getHash()));
				corruptBlocks++;
			}
		}
		return corruptBlocks;
	}

	private void checkDedupFile(File mapFile) throws IOException {
		DataMapInterface mp = null;
		try {
			mp = new LongByteArrayMap(mapFile.getPath());
			long prevpos = 0;
			ArrayList<SparseDataChunk> chunks = new ArrayList<SparseDataChunk>(
					MAX_BATCH_SIZE);
			byte[] val = new byte[0];
			mp.iterInit();
			long corruptBlocks = 0;
			while (val != null) {
				fEvt.curCt += (mp.getIterPos() - prevpos);
				prevpos = mp.getIterPos();
				val = mp.nextValue();
				if (val != null) {
					SparseDataChunk ck = new SparseDataChunk(val,mp.getVersion());
					if (!ck.isLocalData()) {
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
														+ mapFile
														+ "] could not find "
														+ StringUtils
																.getHexString(p.hash));
									corruptBlocks++;
								} else if (SDFSLogger.isDebug()) {
									SDFSLogger
									.getLog()
									.debug("file ["
											+ mapFile
											+ "] found "
											+ StringUtils
													.getHexString(p.hash));
									
								}
							}
						} else {
							chunks.add(ck);
							if (chunks.size() >= MAX_BATCH_SIZE) {
								corruptBlocks += batchCheck(chunks);
								chunks = new ArrayList<SparseDataChunk>(
										MAX_BATCH_SIZE);
							}
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
						"map file " + mapFile.getPath() + " is suspect, ["
								+ corruptBlocks + "] missing blocks found.");
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error(
					"error while checking file [" + mapFile.getPath() + "]", e);
			// throw new IOException(e);
		} finally {
			mp.close();
			mp = null;
		}
		this.files.incrementAndGet();
	}

	private static class CheckDedupFile implements Runnable {

		FDisk fd = null;
		File f = null;

		protected CheckDedupFile(FDisk fd, File f) {
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
