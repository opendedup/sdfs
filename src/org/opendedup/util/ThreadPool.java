package org.opendedup.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.sdfs.io.WritableCacheBuffer;

public class ThreadPool {

	private LinkedBlockingQueue<WritableCacheBuffer> taskQueue = null;
	private List<PoolThread> threads = new ArrayList<PoolThread>();
	private boolean isStopped = false;
	private transient static Logger log = Logger.getLogger("sdfs");

	public ThreadPool(int noOfThreads, int maxNoOfTasks) {
		taskQueue = new LinkedBlockingQueue<WritableCacheBuffer>(maxNoOfTasks);

		for (int i = 0; i < noOfThreads; i++) {
			threads.add(new PoolThread(taskQueue));
		}
		for (PoolThread thread : threads) {
			thread.start();
		}
	}

	public synchronized void execute(WritableCacheBuffer task)
			{
		if (this.isStopped) {
			log.warning("threadpool is stopped will not execute task");
			return;
		}
			
		try {
			this.taskQueue.put(task);
		} catch (InterruptedException e) {
			log.log(Level.WARNING, "thread interrupted",e);
		}
	}

	public synchronized void stop() {
		this.isStopped = true;
		for (PoolThread thread : threads) {
			thread.exit();
		}
	}

}
