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

import java.text.DecimalFormat;

import java.text.NumberFormat;
import java.util.Locale;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class PFullGC implements GCControllerImpl {

	double prevPFull = 0;
	double nextPFull = .05;

	public PFullGC() {
		this.prevPFull = calcPFull();
		this.nextPFull = this.calcNxtRun();
		if (this.nextPFull == 0)
			this.nextPFull = .1;
		double pFull = (this.prevPFull * 100);
		double nFull = (this.nextPFull * 100);
		DecimalFormat twoDForm = (DecimalFormat) NumberFormat
				.getNumberInstance(Locale.US);
		twoDForm.applyPattern("#.##");
		pFull = Double.valueOf(twoDForm.format(pFull));
		nFull = Double.valueOf(twoDForm.format(nFull));
		SDFSLogger.getLog().info(
				"Current DSE Percentage Full is [" + pFull
						+ "] will run GC when [" + nFull + "]");
	}

	@Override
	public void runGC() {
		if(Main.disableAutoGC)
			return;
		if (this.calcPFull() > this.nextPFull) {
			SDFSEvent task = SDFSEvent
					.gcInfoEvent("Percentage Full Exceeded : Running Orphaned Block Collection");
			task.longMsg = "Running Garbage Collection because percentage full is "
					+ this.calcPFull() + " and threshold is " + this.nextPFull;
			try {
				ManualGC.clearChunks(false);
				this.prevPFull = calcPFull();
				this.nextPFull = this.calcNxtRun();
				double pFull = (this.prevPFull * 100);
				double nFull = (this.nextPFull * 100);
				DecimalFormat twoDForm = (DecimalFormat) NumberFormat
						.getNumberInstance(Locale.US);
				twoDForm.applyPattern("#.##");
				pFull = Double.valueOf(twoDForm.format(pFull));
				nFull = Double.valueOf(twoDForm.format(nFull));
				SDFSLogger.getLog().info(
						"Current DSE Percentage Full is [" + pFull
								+ "] will run GC when [" + nFull + "]");
				task.endEvent("Garbage Collection Succeeded");
				task.shortMsg = "Garbage Collection Succeeded";
				task.longMsg = "Current DSE Percentage Full is [" + pFull
						+ "] will run GC when [" + nFull + "]";
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
						/ (double) (HCServiceProxy.getMaxSize()*10);
			}

			return pFull;
	}

	private double calcNxtRun() {
		double next = this.calcPFull();
		if (next >= .92)
			return .90;
		if (next >= 0)
			next = next +.1;
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
		double num = 0.400338958916741818D;
		System.out.println(Math.ceil(num * 10.0));
		System.out.println(Math.ceil(num * 10.0) / 10);
	}

}
