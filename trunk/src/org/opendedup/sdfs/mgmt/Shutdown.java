package org.opendedup.sdfs.mgmt;

import org.opendedup.buse.sdfsdev.VolumeShutdownHook;
import org.opendedup.logging.SDFSLogger;

public class Shutdown implements Runnable {

	public void getResult() throws Exception {
		SDFSLogger.getLog().info("shutting down volume in 10 Seconds");
		System.out.println("shutting down volume in 10 Seconds");
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		try {
			Thread.sleep(10 * 1000);
			VolumeShutdownHook.shutdown();
		} catch (InterruptedException e) {

		}
		System.exit(0);
	}

}
