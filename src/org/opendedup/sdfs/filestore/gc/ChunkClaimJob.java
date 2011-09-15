package org.opendedup.sdfs.filestore.gc;

import java.io.IOException;

import org.opendedup.sdfs.servers.HashChunkService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ChunkClaimJob implements Job {

	@Override
	public void execute(JobExecutionContext ctx) throws JobExecutionException {
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
				long tm = System.currentTimeMillis() - (1 * 60 * 1000);
				//HashChunkService.processHashClaims();
				//HashChunkService.removeStailHashes(tm, true);
			} catch (Exception e) {
				throw new JobExecutionException(e);
			} finally {
				GCMain.unlock();
				GCMain.gclock.unlock();
			}
		}
	}

}
