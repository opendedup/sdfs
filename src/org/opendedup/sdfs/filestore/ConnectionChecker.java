package org.opendedup.sdfs.filestore;

import org.opendedup.logging.SDFSLogger;

import org.opendedup.sdfs.io.Volume;

public class ConnectionChecker implements Runnable {
	AbstractBatchStore store = null;
	Thread th = null;
	int interval = 5000;
	private boolean stopped = false;

	public ConnectionChecker(AbstractBatchStore store, int interval) {

		this.interval = interval;
		if (interval > 0) {
			this.store = store;
			th = new Thread(this);
			th.start();
		}
	}

	@Override
	public void run() {
		while (!stopped) {
			try {
				Volume.setStorageConnected(store.checkAccess());
				if (!Volume.getStorageConnected())
					SDFSLogger.getLog().error("Storage is not connected");

			} catch (Exception e) {
				SDFSLogger.getLog().warn("Error while checking access", e);
			}
			try {
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				break;
			}
		}

	}

	public void stop() {
		this.stopped = true;
		if (th != null)
			th.interrupt();

	}

}
