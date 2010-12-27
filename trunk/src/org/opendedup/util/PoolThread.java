package org.opendedup.util;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.util.SDFSLogger;

import org.opendedup.collections.QuickList;
import org.opendedup.sdfs.io.WritableCacheBuffer;

public class PoolThread extends Thread {

	private BlockingQueue<WritableCacheBuffer> taskQueue = null;
	private boolean isStopped = false;
	private QuickList<WritableCacheBuffer> tasks = new QuickList<WritableCacheBuffer>(35);

	public PoolThread(BlockingQueue<WritableCacheBuffer> queue) {
		taskQueue = queue;
	}

	public void run() {
		while (!isStopped()) {
			try {
				tasks.clear();
				int ts = taskQueue.drainTo(tasks,30);
				for (int i = 0; i < ts; i++) {
					WritableCacheBuffer runnable = tasks.get(i);
					try {
						runnable.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				Thread.sleep(1);
			} catch (Exception e) {
				SDFSLogger.getLog().fatal("unable to execute thread", e);
				// SDFSLogger.getLog() or otherwise report exception,
				// but keep pool thread alive.
			}
		}
	}

	private ReentrantLock exitLock = new ReentrantLock();

	public void exit() {
		exitLock.lock();
		try {
			isStopped = true;
			this.interrupt(); // break pool thread out of dequeue() call.
		} finally {
			exitLock.unlock();
		}
	}

	public boolean isStopped() {
		return isStopped;
	}
}