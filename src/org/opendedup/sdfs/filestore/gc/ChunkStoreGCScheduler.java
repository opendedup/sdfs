package org.opendedup.sdfs.filestore.gc;

import java.util.Date;
import java.util.Properties;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;

public class ChunkStoreGCScheduler {

	Scheduler sched = null;

	public ChunkStoreGCScheduler() {
		try {
			SDFSLogger.getLog().info("Scheduling Garbage Collection Jobs");
			Properties props = new Properties();
			props.setProperty("org.quartz.scheduler.skipUpdateCheck", "true");
			props.setProperty("org.quartz.threadPool.class",
					"org.quartz.simpl.SimpleThreadPool");
			props.setProperty("org.quartz.threadPool.threadCount", "1");
			props.setProperty("org.quartz.threadPool.threadPriority",
					Integer.toString(Thread.MIN_PRIORITY));
			SchedulerFactory schedFact = new StdSchedulerFactory(props);
			sched = schedFact.getScheduler();
			sched.start();
			JobDetail ccjobDetail = new JobDetail("claimChunks", null,
					ChunkClaimJob.class);
			CronTrigger cctrigger = new CronTrigger("claimChunksTrigger",
					"group1", Main.gcChunksSchedule); // fire every hour
			cctrigger.setStartTime(TriggerUtils.getEvenMinuteDate(new Date()));
			cctrigger.setName("claimChunksTrigger");
			sched.scheduleJob(ccjobDetail, cctrigger);
			SDFSLogger.getLog().info("Garbage Collection Jobs Scheduled");
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to schedule Garbage Collection",
					e);
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
