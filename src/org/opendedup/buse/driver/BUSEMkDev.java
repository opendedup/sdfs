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

import java.util.logging.Logger;

public class BUSEMkDev {

	private static Logger log = Logger.getLogger(BUSEMkDev.class.getName());

	static {
		System.loadLibrary("jbuse");
	}

	public static int startdev(final String dev, long sz, int blksz, BUSE buse,
			boolean readonly) throws Exception {

		log.info("Mounted filesystem");
		// ShutdownHook t = new ShutdownHook(dev,buse);
		// Runtime.getRuntime().addShutdownHook(t);
		int z = mkdev(dev, sz, blksz, buse, readonly);

		log.info("Filesystem is unmounted");
		return z;
	}

	public static void closeDev(final String dev) throws Exception {
		closedev(dev);
	}

	public static void init() {
		ThreadGroup threadGroup = new ThreadGroup(Thread.currentThread()
				.getThreadGroup(), "BUSE Threads");
		threadGroup.setDaemon(true);
		init(threadGroup);
	}

	private static native int mkdev(String dev, long sz, int blksz, BUSE buse,
			boolean readonly) throws Exception;

	private static native void closedev(String dev) throws Exception;

	private static native void init(ThreadGroup threadGroup);

	public static native void release();

	public static native void setSize(String dev, long sz);

}
