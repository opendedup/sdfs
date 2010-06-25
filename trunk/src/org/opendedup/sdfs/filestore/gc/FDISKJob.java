package org.opendedup.sdfs.filestore.gc;

import org.opendedup.util.SDFSLogger;




import org.opendedup.mtools.FDisk;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class FDISKJob implements Job {
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		try {
		new FDisk();
		}catch(Exception e) {
			SDFSLogger.getLog().warn(
					"unable to finish executing fdisk", e);
		}

	}

}
