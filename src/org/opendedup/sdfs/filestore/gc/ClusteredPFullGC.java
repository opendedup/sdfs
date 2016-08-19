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
package org.opendedup.sdfs.filestore.gc;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class ClusteredPFullGC implements GCControllerImpl {

	double prevPFull = 0;
	double nextPFull = .05;

	public ClusteredPFullGC() {
		this.prevPFull = calcPFull();
		this.nextPFull = Math.ceil(this.prevPFull * 10) / 10;

		SDFSLogger.getLog().info(
				"Current DSE Percentage Full is [" + this.prevPFull
						+ "] will run GC when [" + this.nextPFull + "]");
	}

	@Override
	public void runGC() {
		if (this.calcPFull() >= this.nextPFull) {
			SDFSEvent task = SDFSEvent
					.gcInfoEvent("Percentage Full Exceeded : Running Orphaned Block Collection");
			task.longMsg = "Running Garbage Collection because percentage full is "
					+ this.calcPFull() + " and threshold is " + this.nextPFull;
			try {
				ManualGC.clearChunks();
				this.prevPFull = calcPFull();
				this.nextPFull = this.calcNxtRun();
				SDFSLogger.getLog()
						.info("Current DSE Percentage Full is ["
								+ this.prevPFull + "] will run GC when ["
								+ this.nextPFull + "]");
				task.endEvent("Garbage Collection Succeeded");
				task.shortMsg = "Garbage Collection Succeeded";
				task.longMsg = "Current DSE Percentage Full is ["
						+ this.prevPFull + "] will run GC when ["
						+ this.nextPFull + "]";
			} catch (Exception e) {
				SDFSLogger.getLog().error("Garbage Collection failed", e);
				task.endEvent(
						"Garbage Collection failed because " + e.getMessage(),
						SDFSEvent.ERROR);
			}
		}

	}

	private double calcPFull() {
		double pFull = 0;
		if (HCServiceProxy.getSize() > 0) {
			pFull = (double) HCServiceProxy.getSize()
					/ (double) HCServiceProxy.getMaxSize();
		}
		return pFull;
	}

	private double calcNxtRun() {
		double next = this.calcPFull();
		if (next >= .92)
			return .90;
		else {
			next = Math.ceil(next * 10.0) / 10;
		}
		if (next == 0)
			next = .1;
		return next;
	}

	@Override
	public void reCalc() {
		this.prevPFull = calcPFull();
		this.nextPFull = this.calcNxtRun();
		SDFSLogger.getLog().debug(
				"Current DSE Percentage Full is [" + this.prevPFull
						+ "] will run GC when [" + this.nextPFull + "]");
	}

	public static void main(String[] args) {
		double num = 0.800338958916741818D;

		System.out.println(Math.ceil(num * 10.0) / 10);
	}

}
