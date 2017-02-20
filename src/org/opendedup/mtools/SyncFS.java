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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.io.events.MFileSync;
import org.opendedup.sdfs.io.events.SFileSync;
import org.opendedup.sdfs.notification.FDiskEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.FileCounts;

import com.google.common.eventbus.EventBus;

public class SyncFS {
	private AtomicLong files = new AtomicLong(0);
	private AtomicLong errorfiles = new AtomicLong(0);
	private FDiskEvent fEvt = null;
	private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient BlockingQueue<Runnable> worksQueue = new ArrayBlockingQueue<Runnable>(
			2);
	private transient ThreadPoolExecutor executor = new ThreadPoolExecutor(
			Main.writeThreads + 1, Main.writeThreads + 1, 10, TimeUnit.SECONDS,
			worksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY),
			executionHandler);
	private static EventBus eventBus = new EventBus();

	public SyncFS() throws IOException {
		fEvt = SDFSEvent
				.fdiskInfoEvent("Starting Cloud Storage Conistancy Check for "
						+ Main.volume.getName() );
		this.init();
	}

	public SyncFS(String now) throws IOException {
		fEvt = SDFSEvent
				.fdiskInfoEvent("Starting Cloud Storage Conistancy Check for "
						+ Main.volume.getName() );
	}

	public static void registerListener(Object obj) {
		eventBus.register(obj);
	}

	public void init() throws IOException {
		File f = new File(Main.volume.getPath());
		long entries = FileCounts.getCount(f, false);
		try {
			long sz = FileCounts.getSize(f, false);
			fEvt.maxCt = sz;
			if (fEvt.maxCt == 0)
				fEvt.maxCt = 1;

			SDFSLogger.getLog().info("entries = " + entries);
			SDFSLogger.getLog().info(
					"Starting Cloud Storage Conistancy Check for "
							+ Main.volume.getName() + " file size = " + sz);
			long start = System.currentTimeMillis();

			this.traverse(f);
			executor.shutdown();
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				SDFSLogger
						.getLog()
						.debug("Awaiting Cloud Storage Conistancy Check completion of threads.");
			}
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000
							+ "] seconds to check ["
							+ (files.get() + this.errorfiles.get())
							+ "] files. errors when checking ["
							+ errorfiles.get() + "] files");

			fEvt.endEvent("took [" + (System.currentTimeMillis() - start)
					/ 1000 + "] seconds to check ["
					+ (files.get() + this.errorfiles.get())
					+ "] files. Errors checking [" + errorfiles.get()
					+ "] files");
		} catch (Exception e) {
			SDFSLogger.getLog()
					.info("Cloud Storage Conistancy Check failed", e);
			fEvt.endEvent(
					"Cloud Storage Conistancy Check Failed because ["
							+ e.toString() + "]", SDFSEvent.ERROR);

		}
	}

	private void traverse(File dir) throws IOException {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				traverse(new File(dir, children[i]));
			}
		} else {
			executor.execute(new CheckDedupFile(this, dir));
		}
	}

	public SDFSEvent getEvt() {
		return this.fEvt;
	}

	ReentrantLock l = new ReentrantLock();

	private void checkDedupFile(MetaDataDedupFile mf) {
		try {
			eventBus.post(new MFileSync(mf));
			if (mf.getDedupFile(false) != null) {
				File directory = new File(Main.dedupDBStore + File.separator
						+ mf.getDfGuid().substring(0, 2) + File.separator
						+ mf.getDfGuid());
				File dbf = new File(directory.getPath() + File.separator
						+ mf.getDfGuid() + ".map");
				eventBus.post(new SFileSync(dbf));
			}

			l.lock();
			fEvt.curCt++;
			l.unlock();
			this.files.incrementAndGet();
		} catch (Exception e) {
			this.errorfiles.incrementAndGet();
		}
	}

	private static class CheckDedupFile implements Runnable {

		SyncFS fd = null;
		File f = null;

		protected CheckDedupFile(SyncFS fd, File f) {
			this.fd = fd;
			this.f = f;
		}

		@Override
		public void run() {
			try {
				fd.checkDedupFile(MetaDataDedupFile.getFile(f.getPath()));
			} catch (IOException e) {
				SDFSLogger.getLog()
				.error("Cloud Storage Conistancy Check failed", e);
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
