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

import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class GCJob implements Job {
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		WriteLock l = GCMain.gclock.writeLock();
		l.lock();
		try {
			SDFSEvent task = SDFSEvent
					.gcInfoEvent("Running Scheduled Volume Garbage Collection");
			try {
				ManualGC.clearChunks(false);
				task.endEvent("Garbage Collection Succeeded");
			} catch (Exception e) {
				SDFSLogger.getLog().error("Garbage Collection failed", e);
				task.endEvent("Garbage Collection failed", SDFSEvent.ERROR, e);
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to finish executing fdisk", e);
		} finally {
			l.unlock();
		}

	}
}
