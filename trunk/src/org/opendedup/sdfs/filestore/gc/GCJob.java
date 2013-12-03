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
				ManualGC.clearChunks(1);
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
