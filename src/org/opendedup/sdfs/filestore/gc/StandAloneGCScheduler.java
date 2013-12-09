package org.opendedup.sdfs.filestore.gc;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class StandAloneGCScheduler implements Runnable {
	private GCControllerImpl gcController = null;
	private boolean closed = false;
	Thread th = null;
	public SDFSGCScheduler gcSched = null;

	public void recalcScheduler() {
		gcController.reCalc();
	}

	public StandAloneGCScheduler() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException {
		gcController = (GCControllerImpl) Class.forName(Main.gcClass)
				.newInstance();
		SDFSLogger.getLog().info(
				"Using " + Main.gcClass + " for DSE Garbage Collection");
		th = new Thread(this);
		try {
			th.setPriority(Thread.MAX_PRIORITY);
		} catch (Throwable e) {
			SDFSLogger.getLog().info(
					"unable to set priority for Standalone GC Sceduler ");
		}
		SDFSLogger.getLog().info("GC Thread priority is " + th.getPriority());
		th.start();
		gcSched = new SDFSGCScheduler();
	}

	@Override
	public void run() {
		while (!closed) {
			gcController.runGC();
			try {
				Thread.sleep(30 * 1000);
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
