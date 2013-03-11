package org.opendedup.util;

import java.util.concurrent.BlockingQueue;


import java.util.concurrent.locks.ReentrantLock;


import org.opendedup.collections.QuickList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.io.DedupChunkInterface;

public class PoolThread extends Thread {

	private BlockingQueue<DedupChunkInterface> taskQueue = null;
	private boolean isStopped = false;
	private final QuickList<DedupChunkInterface> tasks = new QuickList<DedupChunkInterface>(
			60);

	public PoolThread(BlockingQueue<DedupChunkInterface> queue) {
		taskQueue = queue;
	}

	@Override
	public void run() {
		while (!isStopped()) {
			try {
				tasks.clear();
				int ts = taskQueue.drainTo(tasks, 40);
				for (int i = 0; i < ts; i++) {
					DedupChunkInterface runnable = tasks.get(i);
					try {
						runnable.close();
					} catch (Exception e) {
						SDFSLogger.getLog()
								.fatal("unable to execute thread", e);
					}
				}
				Thread.sleep(1);
			} catch (Exception e) {
				SDFSLogger.getLog().fatal("unable to execute thread", e);
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
