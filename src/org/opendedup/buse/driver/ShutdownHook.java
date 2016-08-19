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
package org.opendedup.buse.driver;

public class ShutdownHook extends Thread {
	public String dev;
	public BUSE bClass;

	ShutdownHook(String dev, BUSE bClass) {
		this.bClass = bClass;
		this.dev = dev;
		System.out.println("Registered shutdown hook for " + dev);
	}

	@Override
	public void run() {
		System.out.println("#### Shutting down dev " + dev + " ####");

		try {
			BUSEMkDev.closeDev(dev);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bClass.close();
		System.out.println("Shut Down " + dev);
	}
}
