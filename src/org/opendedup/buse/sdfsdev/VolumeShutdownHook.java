package org.opendedup.buse.sdfsdev;

import org.opendedup.buse.driver.BUSEMkDev;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.SDFSService;

public class VolumeShutdownHook extends Thread {
	public static SDFSService service;
	private static boolean stopped;

	public VolumeShutdownHook() {
	}

	@Override
	public void run() {

		shutdown();
	}

	public static synchronized void shutdown() {
		if (!stopped) {
			stopped = true;
			SDFSLogger.getLog().info("Please Wait while shutting down SDFS");
			SDFSLogger.getLog().info("Data Can be lost if this is interrupted");
			try {
				Main.volume.closeAllDevices();
				Thread.sleep(1000);
				BUSEMkDev.release();
				service.stop();
			} catch (Throwable e) {
				e.printStackTrace();
			}
			SDFSLogger.getLog().info("SDFS Shut Down Cleanly");
		}
	}

}
