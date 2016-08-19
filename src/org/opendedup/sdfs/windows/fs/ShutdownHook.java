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
package org.opendedup.sdfs.windows.fs;

import net.decasdev.dokan.Dokan;

import org.opendedup.sdfs.servers.SDFSService;
import org.opendedup.sdfs.windows.utils.DriveIcon;

class ShutdownHook extends Thread {
	private SDFSService service;
	private String driveLetter;

	public ShutdownHook(SDFSService service, String driveLetter) {
		this.service = service;
		this.driveLetter = driveLetter;
	}

	@Override
	public void run() {

		System.out.println("Please Wait while shutting down SDFS");
		System.out.println("Data Can be lost if this is interrupted");
		service.stop();
		System.out.println("All Data Flushed");
		try {
			System.out.println("Unmounting " + this.driveLetter);
			Dokan.removeMountPoint(driveLetter);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				DriveIcon.deleteIcon(driveLetter);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("SDFS Shut Down Cleanly");

	}
}
