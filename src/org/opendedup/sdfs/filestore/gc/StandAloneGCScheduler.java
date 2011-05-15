package org.opendedup.sdfs.filestore.gc;

import org.opendedup.sdfs.Main;

public class StandAloneGCScheduler implements Runnable {
	private GCControllerImpl gcController = null;
	private boolean closed = false;
	Thread th = null;

	public StandAloneGCScheduler() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		gcController = (GCControllerImpl)Class.forName(Main.gcClass).newInstance();
		th = new Thread(this);
		th.start();
	}
	
	public void run() {
		while(!closed) {
			gcController.runGC();
			try {
				Thread.sleep(5000);
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
