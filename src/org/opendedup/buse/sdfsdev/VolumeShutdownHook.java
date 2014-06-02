package org.opendedup.buse.sdfsdev;

import org.opendedup.buse.driver.BUSEMkDev;
import org.opendedup.logging.SDFSLogger;

import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.servers.SDFSService;

public class VolumeShutdownHook extends Thread {
	private static SDFSService service;
	private static Volume vol;
	private static boolean stopped;

	public VolumeShutdownHook(SDFSService service, Volume vol) {
		VolumeShutdownHook.vol = vol;
		VolumeShutdownHook.service = service;
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
				vol.closeAllDevices();
				Thread.sleep(1000);
				BUSEMkDev.release();
				service.stop();
			} catch (Exception e) {
				e.printStackTrace();
			}
			SDFSLogger.getLog().info("SDFS Shut Down Cleanly");
		}
	}

}
