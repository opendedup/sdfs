package org.opendedup.fsync;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.SyncFS;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class GCJob implements Job {
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		try {
			try {
				new SyncFS();
			} catch (Exception e) {
				SDFSLogger.getLog().error("SyncFS Job Failed", e);
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to finish executing fdisk", e);
		} finally {
		}

	}
}
