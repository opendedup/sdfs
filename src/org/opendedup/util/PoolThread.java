package org.opendedup.util;

import java.util.concurrent.BlockingQueue;
import org.opendedup.util.SDFSLogger;


import org.opendedup.sdfs.io.WritableCacheBuffer;

public class PoolThread extends Thread {

	private BlockingQueue<WritableCacheBuffer> taskQueue = null;
	private boolean isStopped = false;
	

	public PoolThread(BlockingQueue<WritableCacheBuffer> queue) {
		taskQueue = queue;
	}

	public void run() {
		while (!isStopped()) {
			try {
				WritableCacheBuffer runnable = taskQueue.take();
					try {
						runnable.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
			} catch (Exception e) {
				SDFSLogger.getLog().fatal( "unable to execute thread", e);
				// SDFSLogger.getLog() or otherwise report exception,
				// but keep pool thread alive.
			}
		}
	}

	public synchronized void exit() {
		isStopped = true;
		this.interrupt(); // break pool thread out of dequeue() call.
	}

	public synchronized boolean isStopped() {
		return isStopped;
	}
}
