package org.opendedup.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.opendedup.util.SDFSLogger;


import org.opendedup.sdfs.io.WritableCacheBuffer;

public class ThreadPool {

	private ArrayBlockingQueue<WritableCacheBuffer> taskQueue = null;
	private List<PoolThread> threads = new ArrayList<PoolThread>();
	private boolean isStopped = false;
	

	public ThreadPool(int noOfThreads, int maxNoOfTasks) {
		taskQueue = new ArrayBlockingQueue<WritableCacheBuffer>(maxNoOfTasks);

		for (int i = 0; i < noOfThreads; i++) {
			threads.add(new PoolThread(taskQueue));
		}
		for (PoolThread thread : threads) {
			thread.start();
		}
	}

	public void execute(WritableCacheBuffer task)
			{
		if (this.isStopped) {
			SDFSLogger.getLog().warn("threadpool is stopped will not execute task");
			return;
		}
			
		try {
			this.taskQueue.put(task);
		} catch (InterruptedException e) {
			SDFSLogger.getLog().warn( "thread interrupted",e);
		}
	}

	public synchronized void stop() {
		this.isStopped = true;
		for (PoolThread thread : threads) {
			thread.exit();
		}
	}

}
