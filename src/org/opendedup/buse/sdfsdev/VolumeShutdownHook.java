/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
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
				try {
					BUSEMkDev.release();
				}catch(Exception e) {
					
				}
				service.stop();
			} catch (Throwable e) {
				e.printStackTrace();
			}
			SDFSLogger.getLog().info("SDFS Shut Down Cleanly");
		}
	}

}
