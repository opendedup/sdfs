package org.opendedup.mtools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.FileCounts;
import org.opendedup.util.StringUtils;

public class ThreadedFDisk {

	private ArrayBlockingQueue<File> queue = new ArrayBlockingQueue<File>(
			Main.writeThreads + 1);
	private ArrayList<CheckFileThread> ct = new ArrayList<CheckFileThread>();
	private long files = 0;
	private long corruptFiles = 0;
	private final ReentrantLock zLock = new ReentrantLock();
	private final ReentrantLock rtLock = new ReentrantLock();
	private int runningThreads = 0;
	private SDFSEvent fEvt = null;

	public ThreadedFDisk(SDFSEvent evt) throws IOException {
		File f = new File(Main.dedupDBStore);
		if (!f.exists()) {
			SDFSEvent
					.fdiskInfoEvent(
							"FDisk Will not start because the volume has not been written too",
							evt)
					.endEvent(
							"FDisk Will not start because the volume has not been written too");
			throw new IOException(
					"FDisk Will not start because the volume has not been written too");
		}
		fEvt = SDFSEvent.fdiskInfoEvent(
				"Starting FDISK for " + Main.volume.getName()
						+ " file count = " + FileCounts.getCount(f, false)
						+ " file size = " + FileCounts.getSize(f, false), evt);
		fEvt.maxCt = FileCounts.getSize(f, false);
		SDFSLogger.getLog().info("Starting FDISK");
		long start = System.currentTimeMillis();
		for (int i = 0; i < 4; i++) {
			CheckFileThread th = new CheckFileThread();
			ct.add(th);
		}
		try {
			this.traverse(f);

			while (this.running()) {
				Thread.sleep(10);
			}
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
			throw new IOException(e);
		} finally {
			for (int i = 0; i < ct.size(); i++) {
				ct.get(i).close();
			}
			ct.clear();
		}
		SDFSLogger.getLog().info(
				"took [" + (System.currentTimeMillis() - start) / 1000
						+ "] seconds to check [" + files + "]. Found ["
						+ this.corruptFiles + "] corrupt files");

	}

	private boolean running() {
		zLock.lock();
		try {
			return (queue.size() > 0 && this.runningThreads() > 0);
		} finally {
			zLock.unlock();
		}
	}

	private int runningThreads() {
		rtLock.lock();
		try {
			return this.runningThreads;
		} finally {
			rtLock.unlock();
		}

	}

	private void dRT() {
		rtLock.lock();
		try {
			this.runningThreads--;
		} finally {
			rtLock.unlock();
		}
	}

	private void aRT() {
		rtLock.lock();
		try {
			this.runningThreads++;
		} finally {
			rtLock.unlock();
		}
	}

	private void traverse(File dir) throws IOException {
		if (dir.isDirectory()) {
			try {
				String[] children = dir.list();
				for (int i = 0; i < children.length; i++) {
					traverse(new File(dir, children[i]));
				}
			} catch (Exception e) {
				SDFSLogger.getLog().error("error traversing " + dir.getPath(),
						e);
			}
		} else {
			if (dir.getPath().endsWith(".map")) {
				try {
					SDFSLogger.getLog().debug(
							"adding " + dir.getPath() + " to queue");
					zLock.lock();
					queue.put(dir);
					zLock.unlock();
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
		}
	}

	private class CheckFileThread implements Runnable {
		private final ReentrantLock cLock = new ReentrantLock();
		private boolean closed = false;
		Thread _th = null;

		private CheckFileThread() {
			_th = new Thread(this);
			_th.start();
		}

		@Override
		public void run() {
			while (!closed) {
				try {
					Thread.sleep(1);
					zLock.lock();
					File f = queue.poll();
					aRT();
					zLock.unlock();
					if (f != null) {
						try {
							checkDedupFile(f);
						} catch (IOException e) {
							SDFSLogger.getLog().info(
									"Unable to check file " + f.getPath());
						}
					}
					zLock.lock();
					dRT();
					zLock.unlock();

				} catch (InterruptedException e) {
				}
			}
		}

		private void checkDedupFile(File mapFile) throws IOException {
			cLock.lock();
			LongByteArrayMap mp = new LongByteArrayMap(mapFile.getPath());
			long prevpos = 0;
			try {
				byte[] val = new byte[0];
				mp.iterInit();
				boolean corruption = false;
				long corruptBlocks = 0;
				while (val != null) {
					fEvt.curCt += (mp.getIterFPos() - prevpos);
					prevpos = mp.getIterFPos();
					val = mp.nextValue();
					if (val != null) {
						SparseDataChunk ck = new SparseDataChunk(val,mp.version);
						if (!ck.isLocalData()) {
							byte[] exists = HCServiceProxy.hashExists(
									ck.getHash(), false,
									Main.volume.getClusterCopies());
							if (exists[0] == -1) {
								SDFSLogger.getLog().debug(
										"file ["
												+ mapFile
												+ "] could not find "
												+ StringUtils.getHexString(ck
														.getHash()));
								corruption = true;
								corruptBlocks++;
							}
						}
					}
				}
				if (corruption) {
					corruptFiles++;
					SDFSLogger.getLog()
							.info("map file " + mapFile.getPath()
									+ " is suspect, [" + corruptBlocks
									+ "] missing blocks found.");
				}
			} catch (Exception e) {
				SDFSLogger.getLog()
						.warn("error while checking file [" + mapFile.getPath()
								+ "]", e);
				throw new IOException(e);
			} finally {

				mp.close();
				mp = null;
				cLock.unlock();
			}
			files++;
		}

		private void close() {
			cLock.lock();
			SDFSLogger.getLog().debug("closing CheckFileThread");
			this.closed = true;
			_th.interrupt();
			cLock.unlock();
		}

	}

}
