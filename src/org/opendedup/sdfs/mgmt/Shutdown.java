package org.opendedup.sdfs.mgmt;

import org.opendedup.buse.sdfsdev.VolumeShutdownHook;
import org.opendedup.logging.SDFSLogger;

public class Shutdown implements Runnable {

	public void getResult() throws Exception {
		SDFSLogger.getLog().info("shutting down volume");
		System.out.println("shutting down volume");
		Thread th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
			VolumeShutdownHook.shutdown();
		System.exit(0);
	}

}
