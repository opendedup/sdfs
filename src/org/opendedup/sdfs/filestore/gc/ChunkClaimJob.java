package org.opendedup.sdfs.filestore.gc;

import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ChunkClaimJob implements Job {

	@Override
	public void execute(JobExecutionContext ctx) throws JobExecutionException {
		WriteLock l = GCMain.gclock.writeLock();
		l.lock();
		try {
			long tm = System.currentTimeMillis();
			long mil = - (5 * 60 * 1000);
			SDFSEvent evt = SDFSEvent
					.gcInfoEvent("Running Scheduled Chunk Claim Job");
			HCServiceProxy.processHashClaims(evt);
			long dur = System.currentTimeMillis() - tm;
			HCServiceProxy.removeStailHashes(dur+mil, true, evt);
		} catch (Exception e) {
			throw new JobExecutionException(e);
		} finally {
			l.unlock();
		}
	}

}
