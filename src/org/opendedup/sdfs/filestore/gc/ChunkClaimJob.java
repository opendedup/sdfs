package org.opendedup.sdfs.filestore.gc;

import org.opendedup.sdfs.servers.HashChunkService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ChunkClaimJob implements Job {

	@Override
	public void execute(JobExecutionContext ctx) throws JobExecutionException {
		GCMain.gclock.lock();
		if (GCMain.gcRunning) {
			GCMain.gclock.unlock();
			return;
		} else {
			GCMain.gcRunning = true;
			try {
				HashChunkService.processHashClaims();
				HashChunkService.removeStailHashes();
			} catch (Exception e) {
				throw new JobExecutionException(e);
			} finally {
				GCMain.gcRunning = false;
				GCMain.gclock.unlock();
			}
		}
	}

}
