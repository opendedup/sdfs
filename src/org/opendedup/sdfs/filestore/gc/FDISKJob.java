package org.opendedup.sdfs.filestore.gc;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendedup.mtools.FDisk;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class FDISKJob implements Job {
	private transient static Logger log = Logger.getLogger("sdfs");

	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		try {
		new FDisk();
		}catch(Exception e) {
			log.log(Level.WARNING,
					"unable to finish executing fdisk", e);
		}

	}

}
