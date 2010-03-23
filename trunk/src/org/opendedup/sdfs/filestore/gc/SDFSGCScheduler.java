package org.opendedup.sdfs.filestore.gc;

import java.util.Date;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.sdfs.Main;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.CronTrigger;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;

public class SDFSGCScheduler {

	private static Logger log = Logger.getLogger("sdfs");

	Scheduler sched = null;

	public SDFSGCScheduler() {
		try {
			log.info("Scheduling Garbage Collection Jobs for SDFS");
			SchedulerFactory schedFact = new StdSchedulerFactory();
			sched = schedFact.getScheduler();
			sched.start();
			JobDetail ccjobDetail = new JobDetail("fdisk", null, FDISKJob.class);
			CronTrigger cctrigger = new CronTrigger("fdiskTrigger","group1", Main.fDkiskSchedule);
			sched.scheduleJob(ccjobDetail, cctrigger);
			log.info("Garbage Collection Jobs Scheduled");
		} catch (Exception e) {
			log.log(Level.SEVERE, "Unable to schedule SDFS Garbage Collection", e);
		}
	}

	public void stopSchedules() {
		try {
			sched.unscheduleJob("fdisk", "fdiskTrigger");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

}
