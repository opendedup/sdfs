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
package org.opendedup.hashing;

import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.io.WritableCacheBuffer;

public class ThreadPool {

	private BlockingQueue<WritableCacheBuffer> taskQueue = null;
	private List<AbstractPoolThread> threads = new ArrayList<AbstractPoolThread>();
	private boolean isStopped = false;

	public ThreadPool(int noOfThreads, int maxNoOfTasks) {
		taskQueue = new LinkedBlockingQueue<WritableCacheBuffer>(maxNoOfTasks);

		for (int i = 0; i < noOfThreads; i++) {
			threads.add(new PoolThread(taskQueue));
		}
		for (AbstractPoolThread thread : threads) {
			thread.start();
		}
	}

	public void execute(WritableCacheBuffer task) {
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

	public synchronized void flush() {
		while (!this.taskQueue.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	public synchronized void stops() {
		this.isStopped = true;
		for (AbstractPoolThread thread : threads) {
			thread.exit();
		}
	}

}