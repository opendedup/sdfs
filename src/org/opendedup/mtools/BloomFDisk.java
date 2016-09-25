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
import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.opendedup.collections.ProgressiveFileByteArrayLongMap.KeyBlob;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.hashing.LargeFileBloomFilter;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.HashLocPair;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.notification.FDiskEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class BloomFDisk {
	private AtomicLong files = new AtomicLong(0);
	private FDiskEvent fEvt = null;
	transient LargeFileBloomFilter bf = null;
	private boolean failed = false;
	private transient static RejectedExecutionHandler executionHandler = new BlockPolicy();
	private transient static BlockingQueue<Runnable> worksQueue = new SynchronousQueue<Runnable>();
	private transient static ThreadPoolExecutor executor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads,
			10, TimeUnit.SECONDS, worksQueue, new ProcessPriorityThreadFactory(Thread.MIN_PRIORITY), executionHandler);
	public static boolean closed;

	public BloomFDisk() {

	}

	public BloomFDisk(SDFSEvent evt) throws FDiskException {
		long entries = HCServiceProxy.getSize();
		init(evt, entries);
	}

	public BloomFDisk(SDFSEvent evt, long entries) throws FDiskException {
		init(evt, entries);
	}

	public SDFSEvent getEvt() {
		return this.fEvt;
	}

	public void init(SDFSEvent evt, long entries) throws FDiskException {
		if (entries == 0)
			entries = HCServiceProxy.getSize();
		File f = new File(Main.dedupDBStore);
		if (!f.exists()) {
			SDFSEvent.fdiskInfoEvent("FDisk Will not start because the volume has not been written too", evt)
					.endEvent("FDisk Will not start because the volume has not been written too");
			return;
		}
		try {
			long sz = Main.volume.getFiles();
			fEvt = SDFSEvent.fdiskInfoEvent("Starting BFDISK for " + Main.volume.getName() + " file size = " + sz, evt);
			fEvt.maxCt = sz;

			SDFSLogger.getLog().info("entries = " + entries);
			this.bf = new LargeFileBloomFilter(entries, .1, false);
			SDFSLogger.getLog().info("Starting BloomFilter FDISK for " + Main.volume.getName());
			long start = System.currentTimeMillis();

			this.traverse(f);
			executor.shutdown();
			while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
				SDFSLogger.getLog().debug("Awaiting fdisk completion of threads.");
			}
			if (failed)
				throw new IOException("BFDisk traverse failed");
			SDFSLogger.getLog().info(
					"took [" + (System.currentTimeMillis() - start) / 1000 + "] seconds to check [" + files + "].");

			fEvt.endEvent(
					"took [" + (System.currentTimeMillis() - start) / 1000 + "] seconds to check [" + files + "].");
		} catch (Exception e) {
			SDFSLogger.getLog().info("fdisk failed", e);
			fEvt.endEvent("fdisk failed because [" + e.toString() + "]", SDFSEvent.ERROR);
			this.failed = true;
			this.vanish();
			throw new FDiskException(e);
		}
	}

	public LargeFileBloomFilter getResults() {
		return this.bf;
	}

	private void traverse(File dir) throws IOException {
		if (closed)
			throw new IOException("FDISK Closed");
		if (dir.isDirectory()) {
			if (failed)
				throw new IOException("BFDisk traverse failed");
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				traverse(new File(dir, children[i]));
			}
		} else {
			if (failed)
				throw new IOException("BFDisk traverse failed");
			if (dir.getPath().endsWith(".map")) {
				executor.execute(new CheckDedupFile(this, dir));
			}
		}
	}

	private void checkDedupFile(File mapFile) throws IOException {
		if (closed) {
			this.failed = true;
			return;
		}
		LongByteArrayMap mp = null;
		try {
			mp = new LongByteArrayMap(mapFile.getPath());
			File dbf = new File(mapFile.getPath() + ".mmf");
			if (dbf.exists() && dbf.lastModified() >= mapFile.lastModified()) {
				DB db = null;
				try {
					long tm = System.currentTimeMillis();
					db = DBMaker.fileDB(dbf).concurrencyDisable().fileMmapEnable().fileMmapEnableIfSupported()
							.readOnly().make();
					Set<byte[]> map = db.treeSet("refmap", Serializer.BYTE_ARRAY).open();
					map.forEach(new Consumer<byte[]>() {
						@Override
						public void accept(byte[] b) {
							if (BloomFDisk.closed)
								throw new IllegalArgumentException("fdisk closed");
							else
								bf.put(b);
						}
					});

					db.close();
					long etm = System.currentTimeMillis() - tm;
					SDFSLogger.getLog().info("tm = " + etm);
					synchronized (fEvt) {
						fEvt.curCt++;
					}
					this.files.incrementAndGet();
					if (closed) {
						this.failed = true;
						return;
					} else
						return;
				} catch (Exception e) {
					SDFSLogger.getLog().error("unable to read db", e);
					try {
						db.close();
					} catch (Exception e1) {

					}
					try {
						dbf.delete();
					} catch (Exception e1) {

					}
					if (closed) {
						this.failed = true;
						return;
					}
				}
			}
			if(closed) {
				this.failed = true;
				return;
			}
			boolean createdb = false;
			if (mapFile.length() > (1024 * 1024*5))
				createdb = true;
			// long msz = mapFile.length()/4;
			DB db = null;
			Set<byte[]> map = null;
			if (createdb) {

				dbf.delete();
				db = DBMaker.fileDB(dbf).concurrencyDisable().fileMmapEnable().fileMmapEnableIfSupported()
						.allocateStartSize(1024).allocateIncrement(1024).make();

				map = db.treeSet("refmap", Serializer.BYTE_ARRAY).create();
			}
			byte[] val = new byte[0];
			mp.iterInit();
			while (val != null) {
				if (closed) {
					this.failed = true;
					return;
				}
				val = mp.nextValue();
				if (val != null) {
					SparseDataChunk ck = new SparseDataChunk(val, mp.getVersion());
					List<HashLocPair> al = ck.getFingers();
					for (HashLocPair p : al) {
						bf.put(p.hash);
						if (createdb)
							map.add(p.hash);
					}
				}
			}

			synchronized (fEvt) {
				fEvt.curCt++;
			}
			if (createdb) {
				db.commit();
				db.close();
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().info("error while checking file [" + mapFile.getPath() + "]", e);
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

		protected CheckDedupFile(BloomFDisk fd, File f) {
			this.fd = fd;
			this.f = f;
		}

		@Override
		public void run() {
			try {
				fd.checkDedupFile(f);
			} catch (Exception e) {
				SDFSLogger.getLog().error("error doing fdisk", e);
			}
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

	public void vanish() {
		if (bf != null)
			this.bf.vanish();
	}

}
