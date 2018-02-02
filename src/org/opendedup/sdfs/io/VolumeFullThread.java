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
package org.opendedup.sdfs.io;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.DiskFullEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class VolumeFullThread implements Runnable {
	private final Volume vol;
	private Thread th = null;
	private long duration = 15 * 1000;
	boolean closed = false;
	boolean full = false;

	public VolumeFullThread(Volume vol) {
		this.vol = vol;
		th = new Thread(this);
		th.start();
	}

	@Override
	public void run() {
		while (!closed) {

			try {
				Thread.sleep(duration);
				vol.setVolumeFull(this.isFull());
			} catch (Exception e) {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("Unable to check if full.", e);
				this.closed = true;
			}
		}

	}

	public void createDiskFillEvent(String msg) {
		try {
			if (!vol.isFull()) {
				DiskFullEvent evt = new DiskFullEvent(msg);
				evt.curCt = 0;
				evt.maxCt = 1;
				evt.currentSz = vol.getCurrentSize();
				evt.dseSz = HCServiceProxy.getDSESize();
				evt.dskUsage = vol.pathF.getTotalSpace()
						- vol.pathF.getUsableSpace();
				evt.maxDseSz = HCServiceProxy.getMaxSize();
				evt.maxSz = vol.getCapacity();
				evt.maxDskUsage = vol.pathF.getTotalSpace();
				evt.endEvent();
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("error creating disk full event", e);
		}
	}

	private long offset = Main.CHUNK_LENGTH * 10;

	public synchronized boolean isFull() throws Exception {
		long avail = vol.pathF.getUsableSpace();
		if (avail < (offset)) {
			if(!full) {
			SDFSLogger.getLog().warn(
					"Drive is almost full space left is [" + avail + "]");
			this.createDiskFillEvent("Drive is almost full space left is ["
					+ avail + "]");
			
			}
			this.full = true;
			return true;
		}
		if ((vol.getCurrentSize() + offset) >= vol.getCapacity()) {
			if(!full) {
			SDFSLogger.getLog().warn(
					"Drive is almost full. Current Size ["
							+ vol.getCurrentSize() + "] and capacity is ["
							+ vol.getCapacity() + "]");
			this.createDiskFillEvent("Drive is almost full. Current Size ["
					+ vol.getCurrentSize() + "] and capacity is ["
					+ vol.getCapacity() + "]");
			}
			full = true;
			return true;
		} else if (!Main.ignoreDSEHTSize &&(HCServiceProxy.getDSESize() + offset) >= HCServiceProxy
				.getDSEMaxSize()) {
			if(!full) {
			SDFSLogger.getLog().warn(
					"Drive is almost full. DSE Size ["
							+ HCServiceProxy.getDSESize()
							+ "] and DSE Max Size is ["
							+ HCServiceProxy.getDSEMaxSize() + "]");

			this.createDiskFillEvent("Drive is almost full. DSE Size ["
					+ HCServiceProxy.getDSESize() + "] and DSE Max Size is ["
					+ HCServiceProxy.getDSEMaxSize() + "]");
			}
			full = true;
			return true;
		} else if ((HCServiceProxy.getSize() + 10000) >= HCServiceProxy
				.getMaxSize()) {
			if(!full) {
			SDFSLogger.getLog().warn(
					"Drive is almost full. DSE HashMap Size ["
							+ HCServiceProxy.getSize()
							+ "] and DSE HashMap Max Size is ["
							+ HCServiceProxy.getMaxSize() + "]");
			this.createDiskFillEvent("Drive is almost full. DSE HashMap Size ["
					+ HCServiceProxy.getSize()
					+ "] and DSE HashMap Max Size is ["
					+ HCServiceProxy.getMaxSize() + "]");
			}
			full = true;
			return true;
		} else {
			if(this.full) {
				SDFSLogger.getLog().warn(
						"Drive is no longer full");
				this.createDiskFillEvent("Drive is no longer full");
			}
			this.full = false;
		
			return false;
		}
	}

	public void stop() {
		th.interrupt();
		this.closed = true;
	}

}
