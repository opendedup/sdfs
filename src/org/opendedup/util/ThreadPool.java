package org.opendedup.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.opendedup.sdfs.io.WritableCacheBuffer;

public class ThreadPool {

	private LinkedBlockingQueue<WritableCacheBuffer> taskQueue = null;
	private List<PoolThread> threads = new ArrayList<PoolThread>();
	private boolean isStopped = false;

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
			throws InterruptedException {
		if (this.isStopped)
			throw new IllegalStateException("ThreadPool is stopped");
		this.taskQueue.put(task);
	}

	public synchronized void stop() {
		this.isStopped = true;
		for (PoolThread thread : threads) {
			thread.exit();
		}
	}

}
