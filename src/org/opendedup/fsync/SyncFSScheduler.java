/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.fsync;

import java.util.Properties;

import org.opendedup.logging.SDFSLogger;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

public class SyncFSScheduler {

	Scheduler sched = null;
	CronTrigger cctrigger = null;

	public SyncFSScheduler(String schedule) {
		try {
			Properties props = new Properties();
			props.setProperty("org.quartz.scheduler.skipUpdateCheck", "true");
			props.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
			props.setProperty("org.quartz.threadPool.threadCount", "1");
			props.setProperty("org.quartz.threadPool.threadPriority", Integer.toString(8));
			SDFSLogger.getLog().info("Scheduling SyncFS Job for SDFS");
			SchedulerFactory schedFact = new StdSchedulerFactory(props);
			sched = schedFact.getScheduler();
			sched.start();
			JobBuilder jobBuilder = JobBuilder.newJob(GCJob.class);

			JobDetail ccjobDetail = jobBuilder.withIdentity("syncfromcloud").build();
			CronTrigger cctrigger = TriggerBuilder.newTrigger().withIdentity("gcTrigger2")
					.withSchedule(CronScheduleBuilder.cronSchedule(schedule)).build();
			sched.scheduleJob(ccjobDetail, cctrigger);
			SDFSLogger.getLog().info(
					"Stand Alone Cloud SyncFS Scheduled will run first at " + cctrigger.getNextFireTime().toString());
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to schedule Cloud SyncFS", e);
		}
	}

	public String nextFileTime() {
		return cctrigger.getNextFireTime().toString();
	}

	public String schedule() {
		return cctrigger.getCronExpression();
	}

	public void stopSchedules() {
		try {
			sched.unscheduleJob(new TriggerKey("syncfromcloud"));
			sched.deleteJob(new JobKey("syncfromcloud"));
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to stop schedule", e);
		}
	}

}
