/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.mtools;

import java.io.File;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.notification.FDiskEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.FileCounts;
public class GCDDB {
	private AtomicLong files = new AtomicLong(0);
	private FDiskEvent fEvt = null;
	private boolean failed = false;
	private AtomicLong corruptFiles = new AtomicLong(0);
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	private transient ThreadPoolExecutor executor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads, 10,
			TimeUnit.SECONDS, worksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
	public static boolean closed;

	public GCDDB() {

	}

	public GCDDB(SDFSEvent evt) throws IOException {
		init(evt);
	}

	public SDFSEvent getEvt() {
		return this.fEvt;
	}

	private void init(SDFSEvent evt) throws IOException {
		try {
			File f = new File(Main.dedupDBTrashStore);
			if(!f.exists()) {
				SDFSEvent.fdiskInfoEvent("GCDDB Will not start because no files have been deleted", evt)
						.endEvent("GCDDB Will not start because no files have been deleted");
				return;
			}
			long sz = FileCounts.getCount(f, false);
			if (sz ==0) {
				SDFSEvent.fdiskInfoEvent("GCDDB Will not start because no files have been deleted", evt)
						.endEvent("GCDDB Will not start because no files have been deleted");
				return;
			}
			fEvt = SDFSEvent.fdiskInfoEvent("Starting GCDDB for " + Main.volume.getName() + " file size = " + sz, evt);
			fEvt.maxCt = sz;

			SDFSLogger.getLog().info("entries = " + sz);
			SDFSLogger.getLog().info("Starting GCDDB for " + Main.volume.getName());
			long start = System.currentTimeMillis();
			this.traverse(f);
			executor.shutdown();
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				SDFSLogger.getLog().debug("Awaiting fdisk completion of threads.");
			}
			if (failed)
				throw new IOException("GCDDB traverse failed");
			SDFSLogger.getLog().info("took [" + (System.currentTimeMillis() - start) / 1000 + "] seconds to check ["
					+ files + "] corrupt files [" + corruptFiles.get() + "].");

			fEvt.endEvent("took [" + (System.currentTimeMillis() - start) / 1000 + "] seconds to check [" + files
					+ "].");
			evt.endEvent("took [" + (System.currentTimeMillis() - start) / 1000 + "] seconds to check [" + files
					+ "]. ");
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
			fEvt.endEvent("GCDDB count failed because [" + e.toString() + "]", SDFSEvent.ERROR);
			evt.endEvent("GCDDB count failed because [" + e.toString() + "]", SDFSEvent.ERROR);
			this.failed = true;
			throw new IOException(e);
		}
	}

	private void traverse(File dir) throws IOException {
		if (closed)
			throw new IOException("GCDDB Closed");
		if (dir.isDirectory()) {
			if (failed)
				throw new IOException("GCDDB traverse failed");
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				traverse(new File(dir, children[i]));
			}
		} else {
			if (failed)
				throw new IOException("GCDDB traverse failed");
			executor.execute(new CheckDedupFile(this,dir));
			// MetaDataDedupFile.getFile(dir.getPath())));
		}
	}

	

	private static class CheckDedupFile implements Runnable {

		GCDDB fd = null;
		File f;

		protected CheckDedupFile(GCDDB fd,File f) {
			this.fd = fd;
			this.f = f;
		}

		@Override
		public void run() {
			try {
				this.checkDedupFile();
				SDFSLogger.getLog().info("Dereferenced " + this.f.getPath());
			} catch (Exception e) {
				SDFSLogger.getLog().error("error doing fdisk", e);
			}
		}
		
		private void checkDedupFile() throws IOException {
			LongByteArrayMap mp = null;
			try {
				if (GCDDB.closed) {
					fd.failed = true;
					return;
				}
				mp = LongByteArrayMap.getMap(f);
				if (GCDDB.closed) {
					fd.failed = true;
					return;
				}
				mp.vanish(Main.refCount);
				mp.close();
				f.delete();
				f.getParentFile().delete();
				synchronized (fd.fEvt) {
					fd.fEvt.curCt++;
				}
			} catch (Throwable e) {
				SDFSLogger.getLog().error("error while checking file [" + f.getPath() + "]", e);
				fd.corruptFiles.incrementAndGet();
			} finally {
				mp.close();
				mp = null;
			}
			fd.files.incrementAndGet();
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
