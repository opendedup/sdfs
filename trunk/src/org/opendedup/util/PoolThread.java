package org.opendedup.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.sdfs.io.WritableCacheBuffer;


public class PoolThread extends Thread {

	private BlockingQueue<WritableCacheBuffer> taskQueue = null;
	private boolean isStopped = false;
	private transient static Logger log = Logger.getLogger("sdfs");

	public PoolThread(BlockingQueue<WritableCacheBuffer> queue) {
		taskQueue = queue;
	}

	public void run() {
		while (!isStopped()) {
			try {
				WritableCacheBuffer runnable = null;
					runnable = taskQueue.poll();
				if (runnable == null)
					Thread.sleep(1);
				else {
					try {
						runnable.close();
						
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
			} catch (Exception e) {
				log.log(Level.SEVERE,"unable to execute thread",e);
				// log or otherwise report exception,
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
