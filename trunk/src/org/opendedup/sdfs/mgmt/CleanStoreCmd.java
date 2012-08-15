package org.opendedup.sdfs.mgmt;

import java.io.IOException;


import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.gc.ManualGC;
import org.opendedup.util.SDFSLogger;

public class CleanStoreCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		int minutes = Integer.parseInt(cmd);
		try {
			
			long chunks = ManualGC.clearChunks(minutes);
			if(Main.firstRun){
				Thread.sleep(60*1000);
				chunks = chunks + ManualGC.clearChunks(1);
				Main.firstRun = false;
			}
				
			return "cleanded dedup storage engine of ["
					+ chunks + "] records not claimed in  [" + minutes + "] ";
		} catch (Exception e) {
			SDFSLogger
					.getLog()
					.error("ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in  ["
							+ minutes + "] because :" + e.toString(), e);
			throw new IOException(
					"ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in  ["
							+ minutes + "] because :" + e.toString());
		}
	}

}
