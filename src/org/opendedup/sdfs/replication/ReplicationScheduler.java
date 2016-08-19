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
package org.opendedup.sdfs.replication;

import java.util.Properties;

import org.opendedup.logging.SDFSLogger;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

public class ReplicationScheduler {
	Scheduler sched = null;

	public ReplicationScheduler(String schedule, ReplicationService service) {
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
			JobDataMap dataMap = new JobDataMap();
			dataMap.put("service", service);
			JobDetail ccjobDetail = new JobDetail("replication", null,
					ReplicationJob.class);
			ccjobDetail.setJobDataMap(dataMap);
			CronTrigger cctrigger = new CronTrigger("replicationTrigger",
					"group1", schedule);
			sched.scheduleJob(ccjobDetail, cctrigger);
			SDFSLogger.getLog().info("Replication Job Scheduled");
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("Unable to schedule Replication Job", e);
		}
	}

	public void stopSchedules() {
		try {
			sched.unscheduleJob("replication", "replicationTrigger");
		} catch (Exception e) {

		}
	}

}
