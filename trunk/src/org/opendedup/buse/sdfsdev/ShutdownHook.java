package org.opendedup.buse.sdfsdev;

import org.opendedup.logging.SDFSLogger;

import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.servers.SDFSService;

class ShutdownHook extends Thread {
	private SDFSService service;
	private Volume vol;

	public ShutdownHook(SDFSService service, Volume vol) {
		this.vol = vol;
		this.service = service;
	}

	@Override
	public void run() {

		SDFSLogger.getLog().info("Please Wait while shutting down SDFS");
		SDFSLogger.getLog().info("Data Can be lost if this is interrupted");
		try {
			vol.closeAllDevices();
			service.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
		SDFSLogger.getLog().info("SDFS Shut Down Cleanly");
	}
}
