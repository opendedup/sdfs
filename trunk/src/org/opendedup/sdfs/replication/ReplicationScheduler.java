package org.opendedup.sdfs.replication;

import java.util.Properties;

import org.opendedup.util.SDFSLogger;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

public class ReplicationScheduler {
	Scheduler sched = null;

	public ReplicationScheduler(String schedule) {
		try {
			Properties props = new Properties();
			props.setProperty("org.quartz.scheduler.skipUpdateCheck", "true");
			props.setProperty("org.quartz.threadPool.class",
					"org.quartz.simpl.SimpleThreadPool");
			props.setProperty("org.quartz.threadPool.threadCount", "1");
			props.setProperty("org.quartz.threadPool.threadPriority",
					Integer.toString(Thread.NORM_PRIORITY));
			SDFSLogger.getLog().info("Scheduling Replication Job for SDFS");
			SchedulerFactory schedFact = new StdSchedulerFactory(props);
			sched = schedFact.getScheduler();
			sched.start();
			JobDetail ccjobDetail = new JobDetail("replication", null, ReplicationJob.class);
			CronTrigger cctrigger = new CronTrigger("replicationTrigger", "group1",
					schedule);
			sched.scheduleJob(ccjobDetail, cctrigger);
			SDFSLogger.getLog().info("Replication Job Scheduled");
		} catch (Exception e) {
			SDFSLogger.getLog().fatal(
					"Unable to schedule Replication Job", e);
		}
	}

	public void stopSchedules() {
		try {
			sched.unscheduleJob("replication", "replicationTrigger");
		} catch (Exception e) {

		}
	}

}
