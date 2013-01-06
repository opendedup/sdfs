package org.opendedup.sdfs.replication;


import org.opendedup.logging.SDFSLogger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ReplicationJob implements Job {
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
			try {
				ReplicationService service = (ReplicationService)context.getJobDetail().getJobDataMap().get("service");
				service.replicate();
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to finish executing replication", e);
				throw new JobExecutionException(e);
			} 

	}

}
