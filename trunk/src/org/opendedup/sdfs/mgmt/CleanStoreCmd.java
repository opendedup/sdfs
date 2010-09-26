package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.sdfs.filestore.gc.ManualGC;
import org.opendedup.util.SDFSLogger;

public class CleanStoreCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		int minutes = Integer.parseInt(cmd);
		try {
			ManualGC.clearChunks(minutes);
			return "SUCCESS Clean Store: cleanded dedup storage engine of data not claimed in  [" + minutes + "] ";
		} catch (Exception e) {
			SDFSLogger.getLog().error("ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in  [" + minutes + "] because :" + e.toString(), e);
			throw new IOException("ERROR Clean Store: unable to cleand dedup storage engine of data not claimed in  [" + minutes + "] because :" + e.toString());
		}
	}

}
