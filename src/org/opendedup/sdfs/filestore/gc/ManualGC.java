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

import java.io.IOException;

import java.util.concurrent.locks.Lock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.DedupFileStore;
import org.opendedup.sdfs.notification.FDiskEvent;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class ManualGC {

	
	public static SDFSEvent evt = null;
	private static long lastGC = 0;
	//private static long MINGCTIME = 60*1000*60;
	private static long MINGCTIME = 1;
	public static long clearChunks() throws InterruptedException, IOException {
		return clearChunksMills();
	}

	public static synchronized long clearChunksMills()
			throws InterruptedException, IOException {
		Lock l = null;
		if (Main.chunkStoreLocal) {
			l = GCMain.gclock.writeLock();

		} else {
			l = HCServiceProxy.cs.getLock("fdisk");
		}
		l.lock();
		try {
			
			long rm = 0;
			long dur = System.currentTimeMillis()- lastGC;
			if (Main.disableGC) {
				evt = SDFSEvent
						.gcInfoEvent("SDFS Volume Cleanup not enabled for this volume "
								+ Main.volume.getName());
				evt.maxCt = 100;
				evt.curCt = 0;
				evt.endEvent("SDFS Volume Cleanup not enabled for this volume "
						+ Main.volume.getName());
				try {
					Main.pFullSched.recalcScheduler();
				} catch (Exception e) {
				}
				return 0;
			} else if(dur < MINGCTIME) {
				evt = SDFSEvent
						.gcInfoEvent("SDFS Volume Cleanup already occured in the last 1 hour for "
								+ Main.volume.getName());
				evt.maxCt = 100;
				evt.curCt = 0;
				evt.endEvent("SDFS Volume Cleanup already occured in the last 1 hour for "
						+ Main.volume.getName());
				try {
					Main.pFullSched.recalcScheduler();
				} catch (Exception e) {
				}
				return 0;
			}
			else if (Main.chunkStoreLocal)
				evt = SDFSEvent
						.gcInfoEvent("SDFS Volume Cleanup Initiated for "
								+ Main.volume.getName());
			else
				evt = SDFSEvent
						.gcInfoEvent("SDFS Volume Cleanup Initiated for "
								+ Main.DSEClusterID);
			lastGC = System.currentTimeMillis();
			evt.maxCt = 100;
			evt.curCt = 0;
			try {
				rm = runGC();
			} catch (IOException e) {
				if (!Main.firstRun)
					throw e;
			} finally {
				try {
					Main.pFullSched.recalcScheduler();
				} catch (Exception e) {
				}
			}
			if (Main.firstRun && !Main.refCount) {
				SDFSLogger.getLog().info("Waiting 10 Seconds to run again");
				SDFSEvent wevt = SDFSEvent.waitEvent(
						"Waiting 10 Seconds to run again", evt);
				wevt.maxCt = 10;
				for (int i = 0; i < 10; i++) {
					Thread.sleep(1000);
					wevt.curCt++;
				}
				wevt.endEvent("Done Waiting");
				try {
					rm = rm + runGC();
				} finally {
					try {
						Main.pFullSched.recalcScheduler();
					} catch (Exception e) {
					}
				}
				Main.firstRun = false;
			}
			evt.endEvent("SDFS Volume Cleanup Finished for "
					+ Main.volume.getName());
			return rm;
		} finally {
			l.unlock();
		}
	}

	private static long runGC() throws IOException {
		long rm = 0;
		try {
			if (Main.disableGC)
				return 0;
			if (Main.chunkStoreLocal && Main.volume.getName() != null) {
				if(!Main.refCount) {
					throw new IOException("not implemented");
				}else {
					DedupFileStore.gcRunning(true);
					try {
					evt.curCt = 33;
					rm = HCServiceProxy.processHashClaims(evt);
					evt.curCt = 66;
					}finally {
						DedupFileStore.gcRunning(false);
					}
				}
				
			} else {
				FDiskEvent fevt = SDFSEvent.fdiskInfoEvent(
						"running distributed fdisk", evt);
				HCServiceProxy.runFDisk(fevt);
				evt.curCt = 33;
				HCServiceProxy.processHashClaims(evt, null);
				evt.curCt = 66;
			}

		} catch (Throwable e) {
			SDFSLogger.getLog().warn("unable to finish garbage collection", e);
			evt.endEvent(
					"SDFS Volume Cleanup Failed because " + e.getMessage(),
					SDFSEvent.ERROR);
			evt.success = false;
			throw new IOException(e);
		}
		return rm;
	}

}
