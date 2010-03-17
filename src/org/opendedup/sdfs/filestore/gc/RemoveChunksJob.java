package org.opendedup.sdfs.filestore.gc;

import org.opendedup.sdfs.servers.HashChunkService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class RemoveChunksJob implements Job {

	@Override
	public void execute(JobExecutionContext ctx) throws JobExecutionException {
		try {
			HashChunkService.removeStailHashes();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

}
