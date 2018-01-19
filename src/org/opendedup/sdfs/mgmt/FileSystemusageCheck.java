package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.w3c.dom.Element;

public class FileSystemusageCheck implements Runnable {
	SDFSEvent evt = null;
	private long objsize;

	public Element getResult() throws IOException,
			ParserConfigurationException {
		evt = SDFSEvent.cszEvent("Return FS size");
		Thread th = new Thread(this);
		th.start();
		return evt.toXML();
	}

	@Override
	public void run() {
		try {

            long volumeID = Main.DSEID;            
            objsize = HCServiceProxy.getChunkStore().getAllObjSummary("blocks", volumeID);
            SDFSLogger.getLog().error("Total size =  " + objsize);

		} catch (Exception e) {
			evt.endEvent("unable to fulfill request because " + e.getMessage(),
					SDFSEvent.ERROR);
			SDFSLogger.getLog().error("unable to fulfill request ", e);
		}

	}
}
