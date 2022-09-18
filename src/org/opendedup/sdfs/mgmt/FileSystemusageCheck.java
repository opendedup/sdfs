package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class FileSystemusageCheck implements Runnable {
	SDFSEvent evt = null;

	public SDFSEvent getResult() throws IOException,
			ParserConfigurationException {
		evt = SDFSEvent.cszEvent("Return FS size");
		Thread th = new Thread(this);
		th.start();
		return evt;
	}

	@Override
	public void run() {
		try {

            long volumeID = Main.DSEID;
            HCServiceProxy.getChunkStore().getAllObjSummary("blocks", volumeID);

		} catch (Exception e) {
			evt.endEvent("unable to fulfill request because " + e.getMessage(),
					SDFSEvent.ERROR);
			SDFSLogger.getLog().error("unable to fulfill request ", e);
		}

	}
}
