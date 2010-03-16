package org.opendedup.sdfs.filestore.gc;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;

public class ChunkStoreGCScheduler {

	private static Logger log = Logger.getLogger("sdfs");
	JobDetail jobDetail = null;
	Trigger trigger = null;
	Scheduler sched = null;

	public ChunkStoreGCScheduler() {
		try {
			log.info("Scheduling Garbage Collection Jobs");
			SchedulerFactory schedFact = new StdSchedulerFactory();
			Scheduler sched = schedFact.getScheduler();
			sched.start();
			jobDetail = new JobDetail("claimChunks", null, ChunkClaimJob.class);
			trigger = TriggerUtils.makeMinutelyTrigger(); // fire every hour
			trigger.setStartTime(TriggerUtils.getEvenMinuteDate(new Date())); // start
																				// on
																				// the
																				// next
																				// even
																				// hour
			trigger.setName("claimChunksTrigger");
			sched.scheduleJob(jobDetail, trigger);
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
