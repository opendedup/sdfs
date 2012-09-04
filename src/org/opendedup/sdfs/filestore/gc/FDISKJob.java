package org.opendedup.sdfs.filestore.gc;

import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.SDFSLogger;

import org.opendedup.mtools.FDisk;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class FDISKJob implements Job {
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		GCMain.gclock.lock();
		if (GCMain.isLocked()) {
			GCMain.gclock.unlock();
			return;
		} else {
			try {
				GCMain.lock();
			} catch (IOException e1) {
				throw new JobExecutionException(e1);
			}
			try {
				SDFSEvent evt = SDFSEvent.gcInfoEvent("Running GC on " + Main.volume.getName());
				new FDisk(evt);
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to finish executing fdisk", e);
			} finally {
				GCMain.unlock();
				GCMain.gclock.unlock();
			}
		}

	}

}
