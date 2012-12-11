package org.opendedup.sdfs.filestore.gc;

import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

public class StandAloneGCScheduler implements Runnable {
	private GCControllerImpl gcController = null;
	private boolean closed = false;
	Thread th = null;

	public StandAloneGCScheduler() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		gcController = (GCControllerImpl) Class.forName(Main.gcClass)
				.newInstance();
		SDFSLogger.getLog().info(
				"Using " + Main.gcClass + " for DSE Garbage Collection");
		th = new Thread(this);
		th.start();
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
		this.closed = true;
		th.interrupt();
	}

}
