package org.opendedup.mtools;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.BloomFileByteArrayLongMap.KeyBlob;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;
import org.opendedup.util.LargeBloomFilter;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class BloomFDisk {
	private AtomicLong files = new AtomicLong(0);
	private SDFSEvent fEvt = null;
	private AtomicLong entries = new AtomicLong(0);
	transient LargeBloomFilter bf = null;
	private boolean failed = false;
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(
			2);
	private transient ThreadPoolExecutor executor = new ThreadPoolExecutor(Main.writeThreads + 1,
			Main.writeThreads + 1, 10, TimeUnit.SECONDS, worksQueue,new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY),
			executionHandler);
	
	public BloomFDisk(SDFSEvent evt) throws FDiskException {
		init(evt);
	}

	private void init(SDFSEvent evt) throws FDiskException {
		File f = new File(Main.dedupDBStore);
		if (!f.exists()) {
			SDFSEvent
					.fdiskInfoEvent(
							"FDisk Will not start because the volume has not been written too",
							evt)
					.endEvent(
							"FDisk Will not start because the volume has not been written too");
			return;
		}
		try {
			long sz = FileCounts.getSize(f, false);
			fEvt = SDFSEvent.fdiskInfoEvent(
					"Starting BFDISK for " + Main.volume.getName()
							+ " file size = " + sz,
					evt);
			fEvt.maxCt = sz;
			
			if(Main.chunkStoreLocal)
				this.entries = new AtomicLong(HCServiceProxy.getSize());
			else
				this.entries = new AtomicLong(Main.volume.getActualWriteBytes()
						/ HashFunctionPool.avg_page_size);
			SDFSLogger.getLog().info("entries = " + this.entries.get());
			bf = new LargeBloomFilter( this.entries.get(), .10);
			SDFSLogger.getLog().info(
					"Starting BloomFilter FDISK for " + Main.volume.getName());
			long start = System.currentTimeMillis();

			this.traverse(f);
			executor.shutdown();
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				  SDFSLogger.getLog().debug("Awaiting fdisk completion of threads.");
				}
			if(failed)
				throw new IOException("BFDisk traverse failed");
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check [" + files + "].");

			fEvt.endEvent("took [" + (System.currentTimeMillis() - start)
					/ 1000 + "] seconds to check [" + files + "].");
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
			fEvt.endEvent("fdisk failed because [" + e.toString() + "]",
					SDFSEvent.ERROR);
			this.failed = true;

		}
	}

	public LargeBloomFilter getResults() {
		return this.bf;
	}

	private void traverse(File dir) throws IOException {
		if (dir.isDirectory()) {
			if(failed)
				throw new IOException("BFDisk traverse failed");
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				traverse(new File(dir, children[i]));
			}
		} else {
			if(failed)
				throw new IOException("BFDisk traverse failed");
			if (dir.getPath().endsWith(".map")) {
				executor.execute(new CheckDedupFile(this,dir));
			}
		}
	}

	ReentrantLock l = new ReentrantLock();
	private void checkDedupFile(File mapFile) {
		LongByteArrayMap mp = null;
		try {
			mp = new LongByteArrayMap(mapFile.getPath());
			long prevpos = 0;
			byte[] val = new byte[0];
			mp.iterInit();
			while (val != null) {
				l.lock();
				fEvt.curCt += (mp.getIterPos() - prevpos);
				l.unlock();
				prevpos = mp.getIterPos();
				val = mp.nextValue();
				if (val != null) {
					SparseDataChunk ck = new SparseDataChunk(val,mp.getVersion());
					if (!ck.isLocalData()) {
						List<HashLocPair> al = ck.getFingers();
						for (HashLocPair p : al) {
							bf.put(p.hash);
						}
					}
				}
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().info(
					"error while checking file [" + mapFile.getPath() + "]", e);
			this.failed = true;
		} finally {
			mp.close();
			mp = null;
		}
		this.files.incrementAndGet();
	}

	Funnel<KeyBlob> kbFunnel = new Funnel<KeyBlob>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -1612304804452862219L;

		/**
		 * 
		 */

		@Override
		public void funnel(KeyBlob key, PrimitiveSink into) {
			into.putBytes(key.key);
		}
	};
	
	private static class CheckDedupFile implements Runnable {
		
		BloomFDisk fd = null;
		File f = null;
		protected CheckDedupFile(BloomFDisk fd,File f) {
			this.fd =fd;
			this.f = f;
		}
		@Override
		public void run() {
				fd.checkDedupFile(f);
		}
		
	}
	
	private final static class ProcessPriorityThreadFactory implements ThreadFactory {

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
