package org.opendedup.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

public class BoundedExecutor {
	private final ExecutorService exec;
	private final Semaphore semaphore;

	public BoundedExecutor(int bound) {
		this.exec = Executors.newFixedThreadPool(bound + 1);
		this.semaphore = new Semaphore(bound);
	}

	public void execute(final Runnable command) throws InterruptedException, RejectedExecutionException {
		semaphore.acquire();
		try {
			exec.execute(new Runnable() {
				public void run() {
					try {
						command.run();
					} finally {
						semaphore.release();
					}
				}
			});
		} catch (RejectedExecutionException e) {
			semaphore.release();
			throw e;
		}
	}

}
