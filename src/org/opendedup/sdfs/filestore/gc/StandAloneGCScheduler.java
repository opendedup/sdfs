package org.opendedup.sdfs.filestore.gc;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class StandAloneGCScheduler implements Runnable {
	private GCControllerImpl gcController = null;
	private boolean closed = false;
	Thread th = null;
	SDFSGCScheduler gcSched = null;

	public StandAloneGCScheduler() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		gcController = (GCControllerImpl) Class.forName(Main.gcClass)
				.newInstance();
		SDFSLogger.getLog().info(
				"Using " + Main.gcClass + " for DSE Garbage Collection");
		th = new Thread(this);
		th.start();
		gcSched = new SDFSGCScheduler();
	}

	@Override
	public void run() {
		while (!closed) {
			gcController.runGC();
			try {
				Thread.sleep(60 * 1000);
			} catch (InterruptedException e) {
				closed = true;
			}
		}
	}

	public void close() {
		gcSched.stopSchedules();
		this.closed = true;
		th.interrupt();
	}

}
