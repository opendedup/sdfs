package org.opendedup.sdfs.filestore.gc;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.sdfs.Main;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;

public class ChunkStoreGCScheduler {

	private static Logger log = Logger.getLogger("sdfs");

	Scheduler sched = null;

	public ChunkStoreGCScheduler() {
		try {
			log.info("Scheduling Garbage Collection Jobs");
			SchedulerFactory schedFact = new StdSchedulerFactory();
			sched = schedFact.getScheduler();
			sched.start();
			JobDetail ccjobDetail = new JobDetail("claimChunks", null, ChunkClaimJob.class);
			CronTrigger cctrigger = new CronTrigger("claimChunksTrigger","group1",Main.gcChunksSchedule); // fire every hour
			cctrigger.setStartTime(TriggerUtils.getEvenMinuteDate(new Date())); 
			cctrigger.setName("claimChunksTrigger");
			sched.scheduleJob(ccjobDetail, cctrigger);
			log.info("Garbage Collection Jobs Scheduled");
		} catch (Exception e) {
			log.log(Level.SEVERE, "Unable to schedule Garbage Collection", e);
		}
	}

	public void stopSchedules() {
		try {
			sched.unscheduleJob("claimChunks", "claimChunksTrigger");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

}
