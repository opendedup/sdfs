package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.cmds.SetGCScheduleCmd;
import org.opendedup.sdfs.filestore.gc.SDFSGCScheduler;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class SetGCSchedule {

	public void getResult(String cmd, String schedule) throws IOException {
		if (!org.quartz.CronExpression.isValidExpression(schedule))
			throw new IOException("schedule [" + schedule + "] is not valid");
		try {
			if (Main.chunkStoreLocal) {
				Main.pFullSched.gcSched.stopSchedules();
				Main.fDkiskSchedule = schedule;
				Main.pFullSched.gcSched = new SDFSGCScheduler();
				SDFSLogger.getLog().info("schedule set to [" + schedule + "]");
			} else {
				SetGCScheduleCmd acmd = new SetGCScheduleCmd(schedule);
				acmd.executeCmd(HCServiceProxy.cs);
			}

		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request to change schedule " + schedule,
					e);
			throw new IOException("unable to fulfill request to remove volume "
					+ schedule + " because " + e.toString());
		}
	}

}
