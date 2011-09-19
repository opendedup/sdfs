package org.opendedup.sdfs.replication;


import org.opendedup.util.SDFSLogger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ReplicationJob implements Job {
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
			try {
				ReplicationService.replicate();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to finish executing replication", e);
				throw new JobExecutionException(e);
			} 

	}

}
