package org.opendedup.sdfs.filestore.gc;

import java.io.IOException;

import org.opendedup.sdfs.notification.SDFSEvent;
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
				long tm = System.currentTimeMillis() - (5 * 60 * 1000);
				SDFSEvent evt = SDFSEvent.gcInfoEvent("Running Scheduled Chunk Claim Job");
				HashChunkService.processHashClaims(evt);
				HashChunkService.removeStailHashes(tm, true,evt);
			} catch (Exception e) {
				throw new JobExecutionException(e);
			} finally {
				GCMain.unlock();
				GCMain.gclock.unlock();
			}
		}
	}

}
