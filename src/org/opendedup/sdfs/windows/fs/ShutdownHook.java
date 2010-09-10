package org.opendedup.sdfs.windows.fs;

import net.decasdev.dokan.Dokan;

import org.opendedup.sdfs.servers.SDFSService;

class ShutdownHook extends Thread {
	private SDFSService service;
	private char driveLetter;

	public ShutdownHook(SDFSService service, char driveLetter) {
		this.service = service;
		this.driveLetter = driveLetter;
	}

	public void run() {

		System.out.println("Please Wait while shutting down SDFS");
		System.out.println("Data Can be lost if this is interrupted");
		service.stop();
		System.out.println("All Data Flushed");
		try {
			System.out.println("Unmounting " + this.driveLetter);
			Dokan.unmount(driveLetter);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("SDFS Shut Down Cleanly");
		

	}
}
