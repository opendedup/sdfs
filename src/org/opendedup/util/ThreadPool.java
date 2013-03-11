package org.opendedup.util;

import java.util.ArrayList;



import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;


import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.io.DedupChunkInterface;

public class ThreadPool {

	private LinkedBlockingQueue<DedupChunkInterface> taskQueue = null;
	private List<PoolThread> threads = new ArrayList<PoolThread>();
	private boolean isStopped = false;

	public ThreadPool(int noOfThreads, int maxNoOfTasks) {
		taskQueue = new LinkedBlockingQueue<DedupChunkInterface>(maxNoOfTasks);

		for (int i = 0; i < noOfThreads; i++) {
			threads.add(new PoolThread(taskQueue));
		}
		for (PoolThread thread : threads) {
			thread.start();
		}
	}

	public void execute(DedupChunkInterface task) {
		if (this.isStopped) {
			SDFSLogger.getLog().warn(
					"threadpool is stopped will not execute task");
			return;
		}

		try {
			this.taskQueue.put(task);
		} catch (InterruptedException e) {
			SDFSLogger.getLog().warn("thread interrupted", e);
		}
	}

	public synchronized void stop() {
		this.isStopped = true;
		for (PoolThread thread : threads) {
			thread.exit();
		}
	}

}
